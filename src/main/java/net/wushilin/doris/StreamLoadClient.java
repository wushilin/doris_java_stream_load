package net.wushilin.doris;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.UnknownHostException;
import java.net.http.HttpClient;
import java.net.http.HttpTimeoutException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Thread-safe client for Apache Doris Stream Load.
 *
 * <p>Records submitted via {@link #submit(String)} are buffered into batches (by size or time),
 * then uploaded to Doris by a pool of worker threads. Each accepted submission returns a
 * {@link StreamLoadHandle}; its {@link StreamLoadHandle#resultFuture()} completes with
 * the delivery result.
 *
 * <p>The client is safe for concurrent use. Always call {@link #close()} when done to
 * flush pending records and shut down background threads.
 *
 * <pre>{@code
 * try (StreamLoadClient client = new StreamLoadClient(config)) {
 *     StreamLoadHandle h = client.submit("{\"id\":1,\"name\":\"Alice\"}");
 *     DeliveryResult result = h.waitForResult();
 * }
 * }</pre>
 */
public class StreamLoadClient implements Closeable {

    private static final Logger LOG = Logger.getLogger(StreamLoadClient.class.getName());
    private static final int MAX_REDIRECTS = 10;

    private final StreamLoadConfig config;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final ClosableBlockingQueue<PendingSubmission> intakeQueue;
    private final ClosableBlockingQueue<DeliveryBatch> uploadQueue;

    private final AtomicLong statRecordsSent   = new AtomicLong();
    private final AtomicLong statRecordsFailed = new AtomicLong();
    private final AtomicLong statBatches       = new AtomicLong();
    private final AtomicLong statBatchesFailed = new AtomicLong();
    private final AtomicLong statBytesSent     = new AtomicLong();
    private final AtomicLong statRetries       = new AtomicLong();

    private final Thread batcherThread;
    private final ExecutorService workerPool;
    private final ExecutorService admissionPool;

    private volatile boolean closed = false;

    private static final SecureRandom RANDOM = new SecureRandom();
    private static final int BATCHER_DRAIN_MAX_ITEMS = 1_024;

    public StreamLoadClient(StreamLoadConfig config) {
        this.config = Objects.requireNonNull(config, "config");

        this.intakeQueue = new ClosableBlockingQueue<>(config.getMaxQueueSize());
        this.uploadQueue = new ClosableBlockingQueue<>(config.getMaxUploadQueueSize());

        this.httpClient = buildHttpClient();

        this.batcherThread = new Thread(this::runBatcher, "doris-batcher");
        this.batcherThread.setDaemon(true);
        this.batcherThread.start();

        this.workerPool = Executors.newFixedThreadPool(
                config.getUploadWorkers(), daemonThreadFactory("doris-worker"));
        for (int i = 0; i < config.getUploadWorkers(); i++) {
            workerPool.submit(this::runWorker);
        }

        int admissionThreads = Math.max(2, config.getUploadWorkers());
        this.admissionPool = new ThreadPoolExecutor(
                admissionThreads,
                admissionThreads,
                0L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(config.getMaxQueueSize()),
                daemonThreadFactory("doris-admission"));
    }

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    /**
     * Submits a single record. The returned handle means the SDK accepted the
     * record into its queue; the handle's result future completes with the Doris
     * delivery result.
     */
    public StreamLoadHandle submit(String record) throws StreamLoadException {
        return enqueueSubmission(Collections.singletonList(record), null, null);
    }

    /**
     * Submits a single record with a per-call queue wait timeout. Throws
     * {@link StreamLoadException} with {@code QUEUE_FULL} if the queue is still full
     * after {@code timeout}.
     */
    public StreamLoadHandle submit(String record, Duration timeout) throws StreamLoadException {
        return enqueueSubmission(Collections.singletonList(record), timeout, null);
    }

    /**
     * Submits multiple records as one accepted submission. The records may be
     * coalesced with other submissions into one Doris stream-load request; all
     * records in this submission share one delivery result.
     */
    public StreamLoadHandle submitBatch(List<String> records) throws StreamLoadException {
        return enqueueSubmission(records, null, null);
    }

    /**
     * Submits multiple records with a per-call queue wait timeout.
     */
    public StreamLoadHandle submitBatch(List<String> records, Duration timeout) throws StreamLoadException {
        return enqueueSubmission(records, timeout, null);
    }

    /**
     * Submits a record and invokes {@code callback} when delivery completes.
     */
    public StreamLoadHandle submitWithCallback(String record, Consumer<DeliveryResult> callback)
            throws StreamLoadException {
        return enqueueSubmission(Collections.singletonList(record), null, callback);
    }

    /**
     * Submits multiple records and invokes {@code callback} once for the submission.
     */
    public StreamLoadHandle submitBatchWithCallback(List<String> records,
                                                    Consumer<DeliveryResult> callback)
            throws StreamLoadException {
        return enqueueSubmission(records, null, callback);
    }

    /**
     * Asynchronously performs queue admission for one record.
     */
    public CompletableFuture<StreamLoadHandle> submitAsync(String record) {
        return submitAsync(record, null);
    }

    /**
     * Asynchronously performs queue admission for one record with a per-call queue timeout.
     */
    public CompletableFuture<StreamLoadHandle> submitAsync(String record, Duration timeout) {
        return submitBatchAsync(Collections.singletonList(record), timeout);
    }

    /**
     * Asynchronously performs queue admission for multiple records.
     */
    public CompletableFuture<StreamLoadHandle> submitBatchAsync(List<String> records) {
        return submitBatchAsync(records, null);
    }

    /**
     * Asynchronously performs queue admission for multiple records with a per-call queue timeout.
     */
    public CompletableFuture<StreamLoadHandle> submitBatchAsync(List<String> records, Duration timeout) {
        CompletableFuture<StreamLoadHandle> future = new CompletableFuture<>();
        try {
            admissionPool.execute(() -> {
                try {
                    future.complete(submitBatch(records, timeout));
                } catch (StreamLoadException e) {
                    future.completeExceptionally(e);
                } catch (RuntimeException e) {
                    future.completeExceptionally(e);
                }
            });
        } catch (RejectedExecutionException e) {
            try {
                checkNotClosed();
                future.completeExceptionally(new StreamLoadException(
                        StreamLoadException.ErrorCode.QUEUE_FULL,
                        "Admission executor queue is full", e));
            } catch (StreamLoadException closedError) {
                future.completeExceptionally(closedError);
            }
        }
        return future;
    }

    /** Alias for {@link #submit(String)}. */
    public StreamLoadHandle send(String record) throws StreamLoadException {
        return submit(record);
    }

    /** Alias for {@link #submit(String, Duration)}. */
    public StreamLoadHandle send(String record, Duration timeout) throws StreamLoadException {
        return submit(record, timeout);
    }

    /** Alias for {@link #submitWithCallback(String, Consumer)}. */
    public StreamLoadHandle sendWithCallback(String record, Consumer<DeliveryResult> callback)
            throws StreamLoadException {
        return submitWithCallback(record, callback);
    }

    /** Alias for {@link #submitBatch(List)}. */
    public StreamLoadHandle sendBatch(List<String> records) throws StreamLoadException {
        return submitBatch(records);
    }

    /** Alias for {@link #submitBatchWithCallback(List, Consumer)}. */
    public StreamLoadHandle sendBatchWithCallback(List<String> records,
                                                  Consumer<DeliveryResult> callback)
            throws StreamLoadException {
        return submitBatchWithCallback(records, callback);
    }

    /**
     * Returns a point-in-time snapshot of client metrics.
     */
    public ClientStats stats() {
        return new ClientStats(
                statRecordsSent.get(),
                statRecordsFailed.get(),
                statBatches.get(),
                statBatchesFailed.get(),
                statBytesSent.get(),
                statRetries.get(),
                intakeQueue.size(),
                uploadQueue.size()
        );
    }

    /**
     * Signals shutdown, then waits for accepted submissions to drain through the
     * pipeline before returning.
     */
    @Override
    public void close() {
        closed = true;
        intakeQueue.close();
        admissionPool.shutdown();
        workerPool.shutdown();
        joinUninterruptibly(batcherThread);
        awaitTerminationUninterruptibly(admissionPool);
        awaitTerminationUninterruptibly(workerPool);
    }

    // -------------------------------------------------------------------------
    // Internal: enqueue
    // -------------------------------------------------------------------------

    private StreamLoadHandle enqueueSubmission(List<String> records, Duration callTimeout,
                                                Consumer<DeliveryResult> callback)
            throws StreamLoadException {
        checkNotClosed();

        if (records == null || records.isEmpty()) {
            throw new StreamLoadException(StreamLoadException.ErrorCode.INVALID_RECORD,
                    "At least one record is required");
        }

        CompletableFuture<DeliveryResult> resultFuture = new CompletableFuture<>();
        List<PendingRecord> pendingRecords = new ArrayList<>(records.size());
        int byteSize = 0;

        for (int i = 0; i < records.size(); i++) {
            String record = records.get(i);
            validateRecord(record);
            byte[] data = record.getBytes(StandardCharsets.UTF_8);
            byteSize += data.length;
            if (i > 0) byteSize++;
            pendingRecords.add(new PendingRecord(data, resultFuture));
        }
        if (config.getMode() == Mode.JSON) {
            byteSize += 2;
        }

        if (byteSize > config.getBatchBytes()) {
            throw new StreamLoadException(StreamLoadException.ErrorCode.RECORD_TOO_LARGE,
                    "Submission is " + byteSize + " bytes, larger than batchBytes="
                            + config.getBatchBytes());
        }

        Optional<Duration> waitTime = Optional.ofNullable(
                callTimeout != null ? callTimeout : config.getMaxQueueWaitTime());
        try {
            enqueueOrFail(new PendingSubmission(pendingRecords, resultFuture, callback, byteSize), waitTime);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new StreamLoadException(StreamLoadException.ErrorCode.INTERNAL,
                    "Interrupted while waiting for intake queue", e);
        }

        return new StreamLoadHandle(resultFuture, records.size(), byteSize, Instant.now());
    }

    private void enqueueOrFail(PendingSubmission submission, Optional<Duration> waitTime)
            throws InterruptedException, StreamLoadException {
        if (waitTime.isEmpty()) {
            checkNotClosed();
            ClosableBlockingQueue.OfferResult result = intakeQueue.put(submission);
            if (result.ok()) {
                return;
            }
            throw clientClosed();
        }

        Duration timeout = waitTime.orElseThrow();
        if (timeout.isNegative()) {
            throw new StreamLoadException(StreamLoadException.ErrorCode.INVALID_CONFIG,
                    "Queue wait timeout must be >= 0");
        }
        long timeoutNanos = timeout.toNanos();
        if (timeoutNanos <= 0) {
            checkNotClosed();
            ClosableBlockingQueue.OfferResult result =
                    intakeQueue.offer(submission, 0, TimeUnit.NANOSECONDS);
            if (result.ok()) {
                return;
            }
            if (result.isClosed()) {
                throw clientClosed();
            }
            throw queueFull(timeout);
        }

        checkNotClosed();
        ClosableBlockingQueue.OfferResult result =
                intakeQueue.offer(submission, timeoutNanos, TimeUnit.NANOSECONDS);
        if (result.ok()) {
            return;
        }
        if (result.isClosed()) {
            throw clientClosed();
        }
        throw queueFull(timeout);
    }

    private StreamLoadException queueFull(Duration waitTime) {
        return new StreamLoadException(StreamLoadException.ErrorCode.QUEUE_FULL,
                "Intake queue is full - submission rejected after "
                        + waitTime.toMillis() + " ms");
    }

    private void checkNotClosed() throws StreamLoadException {
        if (closed) {
            throw clientClosed();
        }
    }

    private StreamLoadException clientClosed() {
        return new StreamLoadException(StreamLoadException.ErrorCode.CLIENT_CLOSED,
                "StreamLoadClient is already closed");
    }

    private void validateRecord(String record) throws StreamLoadException {
        if (record == null) {
            throw new StreamLoadException(StreamLoadException.ErrorCode.INVALID_RECORD,
                    "Record must not be null");
        }
        // Reject blank records (matches Go behavior)
        if (record.isBlank()) {
            throw new StreamLoadException(StreamLoadException.ErrorCode.INVALID_RECORD,
                    "Record must not be blank");
        }
        if (config.getValidationMode() == ValidationMode.NONE) return;

        if (config.getMode() == Mode.JSON) {
            try {
                JsonNode node = objectMapper.readTree(record);
                if (node == null || !node.isObject()) {
                    throw new StreamLoadException(StreamLoadException.ErrorCode.INVALID_RECORD,
                            "Record is not a JSON object");
                }
                if (config.getValidationMode() == ValidationMode.STRICT && !config.getColumns().isEmpty()) {
                    Set<String> declared = new HashSet<>(config.getColumns());
                    for (String col : config.getColumns()) {
                        if (!node.has(col)) {
                            throw new StreamLoadException(StreamLoadException.ErrorCode.INVALID_RECORD,
                                    "JSON record is missing required column: " + col);
                        }
                    }
                    Iterator<String> fieldNames = node.fieldNames();
                    while (fieldNames.hasNext()) {
                        String field = fieldNames.next();
                        if (!declared.contains(field)) {
                            throw new StreamLoadException(StreamLoadException.ErrorCode.INVALID_RECORD,
                                    "JSON record has unexpected column: " + field);
                        }
                    }
                }
            } catch (StreamLoadException e) {
                throw e;
            } catch (Exception e) {
                throw new StreamLoadException(StreamLoadException.ErrorCode.INVALID_RECORD,
                        "Record is not valid JSON: " + e.getMessage());
            }
        } else {
            // CSV validation: check column count when columns are declared
            if (!config.getColumns().isEmpty()) {
                int fieldCount = parseCsvRecord(record);
                if (fieldCount != config.getColumns().size()) {
                    throw new StreamLoadException(StreamLoadException.ErrorCode.INVALID_RECORD,
                            "CSV record has " + fieldCount + " field(s) but "
                                    + config.getColumns().size() + " column(s) are declared");
                }
            }
        }
    }

    /**
     * Counts CSV fields in one logical record using the configured CSV separator
     * and optional one-character CSV quote/enclose setting.
     */
    private int parseCsvRecord(String record) throws StreamLoadException {
        String sep = config.getCsvSeparator();
        String enclose = config.getCsvEnclose();
        char quoteChar = enclose == null || enclose.isEmpty() ? 0 : enclose.charAt(0);
        int count = 1;
        boolean inQuotes = false;
        boolean atFieldStart = true;

        for (int i = 0; i < record.length(); i++) {
            char c = record.charAt(i);
            if (quoteChar != 0 && c == quoteChar) {
                if (inQuotes) {
                    if (i + 1 < record.length() && record.charAt(i + 1) == quoteChar) {
                        i++;
                    } else {
                        inQuotes = false;
                    }
                } else if (atFieldStart) {
                    inQuotes = true;
                } else {
                    throw new StreamLoadException(StreamLoadException.ErrorCode.INVALID_RECORD,
                            "Invalid CSV quote in unquoted field");
                }
                atFieldStart = false;
                continue;
            }
            if (!inQuotes && record.startsWith(sep, i)) {
                count++;
                i += sep.length() - 1;
                atFieldStart = true;
                continue;
            }
            if (!inQuotes && (c == '\n' || c == '\r')) {
                throw new StreamLoadException(StreamLoadException.ErrorCode.INVALID_RECORD,
                        "CSV record must contain exactly one row");
            }
            atFieldStart = false;
        }
        if (inQuotes) {
            throw new StreamLoadException(StreamLoadException.ErrorCode.INVALID_RECORD,
                    "CSV record has unterminated quoted field");
        }
        return count;
    }

    // -------------------------------------------------------------------------
    // Batcher thread
    // -------------------------------------------------------------------------

    private void runBatcher() {
        List<PendingRecord> batchRecords = new ArrayList<>();
        List<PendingSubmission> batchSubmissions = new ArrayList<>();
        List<PendingSubmission> drainedSubmissions = new ArrayList<>();
        ByteArrayOutputStream payloadBuf = new ByteArrayOutputStream();
        long batchDeadline = 0;
        int nextDrainedSubmissionIndex = 0;

        try {
            while (true) {
                // When the batch is empty, block indefinitely until the first item arrives so
                // that the linger window is measured from when that first item lands, not from
                // when the previous batch was flushed.
                boolean batchEmpty = batchRecords.isEmpty();
                long drainTimeoutNs = batchEmpty
                        ? Long.MAX_VALUE
                        : Math.max(0, batchDeadline - System.nanoTime());

                drainedSubmissions.clear();
                nextDrainedSubmissionIndex = 0;
                ClosableBlockingQueue.DrainResult result =
                        intakeQueue.drainToWithTimeout(
                                drainedSubmissions,
                                BATCHER_DRAIN_MAX_ITEMS,
                                drainTimeoutNs,
                                TimeUnit.NANOSECONDS);
                if (!result.ok()) {
                    if (!batchRecords.isEmpty()) {
                        flush(batchRecords, batchSubmissions, payloadBuf);
                        batchRecords.clear();
                        batchSubmissions.clear();
                        payloadBuf.reset();
                    }
                    if (result.isClosed()) {
                        break;
                    }
                    // Linger expired: batch was flushed above. Next iteration will wait
                    // indefinitely for the first item of the new batch.
                    drainedSubmissions.clear();
                    continue;
                }

                // First items of a new batch just arrived: start the linger clock now.
                if (batchEmpty) {
                    batchDeadline = nextDeadline();
                }

                for (; nextDrainedSubmissionIndex < drainedSubmissions.size();
                     nextDrainedSubmissionIndex++) {
                    PendingSubmission submission = drainedSubmissions.get(nextDrainedSubmissionIndex);
                    if (appendOrFlushSubmission(
                            payloadBuf, batchRecords, batchSubmissions, submission)) {
                        // A size-triggered flush happened; the submission that caused the
                        // overflow is now the first item of the next batch — start its linger.
                        batchDeadline = nextDeadline();
                    }
                }
                drainedSubmissions.clear();
                nextDrainedSubmissionIndex = 0;
            }

            if (!batchRecords.isEmpty()) {
                flush(batchRecords, batchSubmissions, payloadBuf);
            }
        } catch (Throwable e) {
            LOG.log(Level.SEVERE, "Uncaught batcher exception; failing accepted stream-load submissions", e);
            failBatcherSubmissions(
                    batchRecords, batchSubmissions, drainedSubmissions, nextDrainedSubmissionIndex, e);
        } finally {
            uploadQueue.close();
        }
    }

    private boolean appendOrFlushSubmission(ByteArrayOutputStream payloadBuf,
                                            List<PendingRecord> batchRecords,
                                            List<PendingSubmission> batchSubmissions,
                                            PendingSubmission submission) {
        boolean flushed = false;
        if (!batchRecords.isEmpty() && payloadBuf.size()
                + appendedSize(payloadBuf.size(), submission) > config.getBatchBytes()) {
            flush(batchRecords, batchSubmissions, payloadBuf);
            batchRecords.clear();
            batchSubmissions.clear();
            payloadBuf.reset();
            flushed = true;
        }

        appendSubmission(payloadBuf, batchRecords, batchSubmissions, submission);

        if (payloadBuf.size() >= config.getBatchBytes()) {
            flush(batchRecords, batchSubmissions, payloadBuf);
            batchRecords.clear();
            batchSubmissions.clear();
            payloadBuf.reset();
            flushed = true;
        }
        return flushed;
    }

    private long nextDeadline() {
        return System.nanoTime() + config.getLinger().toNanos();
    }

    private int appendedSize(int currentSize, PendingSubmission submission) {
        if (config.getMode() == Mode.JSON) {
            int dataBytes = 0;
            for (PendingRecord record : submission.records) {
                dataBytes += record.data.length;
            }
            // Each record is preceded by '[' (first overall) or ',' (all others).
            return dataBytes + submission.records.size();
        }
        return currentSize == 0 ? submission.byteSize : submission.byteSize + 1;
    }

    private void appendRecord(ByteArrayOutputStream buf, byte[] data, boolean first) {
        try {
            if (config.getMode() == Mode.JSON) {
                buf.write(first ? '[' : ',');
                buf.write(data);
            } else {
                if (!first) buf.write('\n');
                buf.write(data);
            }
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private void appendSubmission(ByteArrayOutputStream payloadBuf,
                                  List<PendingRecord> batchRecords,
                                  List<PendingSubmission> batchSubmissions,
                                  PendingSubmission submission) {
        boolean first = batchRecords.isEmpty();
        batchSubmissions.add(submission);
        for (int i = 0; i < submission.records.size(); i++) {
            PendingRecord record = submission.records.get(i);
            appendRecord(payloadBuf, record.data, first && i == 0);
            batchRecords.add(record);
        }
    }

    private void flush(List<PendingRecord> records,
                       List<PendingSubmission> submissions,
                       ByteArrayOutputStream payloadBuf) {
        byte[] payload;
        if (config.getMode() == Mode.JSON) {
            payloadBuf.write(']');
        }
        payload = payloadBuf.toByteArray();

        DeliveryBatch batch = new DeliveryBatch(new ArrayList<>(records), new ArrayList<>(submissions), payload);
        try {
            ClosableBlockingQueue.OfferResult result = uploadQueue.put(batch);
            if (!result.isAccepted()) {
                throw new AssertionError("uploadQueue.put() returned " + result + " inside flush()");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            Exception err = new StreamLoadException(StreamLoadException.ErrorCode.INTERNAL,
                    "Interrupted while handing batch to upload queue");
            Instant now = Instant.now();
            completeBatch(batch, DeliveryResult.failure(err, 0, 0, now, now));
        }
    }

    private void failBatcherSubmissions(List<PendingRecord> batchRecords,
                                        List<PendingSubmission> batchSubmissions,
                                        Throwable error) {
        failBatcherSubmissions(batchRecords, batchSubmissions, Collections.emptyList(), 0, error);
    }

    private void failBatcherSubmissions(List<PendingRecord> batchRecords,
                                        List<PendingSubmission> batchSubmissions,
                                        List<PendingSubmission> drainedSubmissions,
                                        int firstUnhandledDrainedSubmission,
                                        Throwable error) {
        Instant now = Instant.now();
        DeliveryResult result = DeliveryResult.failure(asException(error), 0, 0, now, now);
        Set<PendingSubmission> completedSubmissions = Collections.newSetFromMap(new IdentityHashMap<>());
        if (!batchSubmissions.isEmpty()) {
            DeliveryBatch batch = new DeliveryBatch(
                    new ArrayList<>(batchRecords), new ArrayList<>(batchSubmissions), new byte[0]);
            completeBatch(batch, result);
            completedSubmissions.addAll(batchSubmissions);
        }

        for (int i = Math.max(0, firstUnhandledDrainedSubmission); i < drainedSubmissions.size(); i++) {
            PendingSubmission submission = drainedSubmissions.get(i);
            if (completedSubmissions.add(submission)) {
                completeBatch(singleSubmissionFailureBatch(submission), result);
            }
        }

        List<PendingSubmission> queuedSubmissions = new ArrayList<>();
        while (true) {
            queuedSubmissions.clear();
            ClosableBlockingQueue.DrainResult drained;
            try {
                drained = intakeQueue.drainToWithTimeout(
                        queuedSubmissions, BATCHER_DRAIN_MAX_ITEMS, 0, TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            if (!drained.ok()) {
                return;
            }
            for (PendingSubmission submission : queuedSubmissions) {
                if (completedSubmissions.add(submission)) {
                    completeBatch(singleSubmissionFailureBatch(submission), result);
                }
            }
        }
    }

    private DeliveryBatch singleSubmissionFailureBatch(PendingSubmission submission) {
        return new DeliveryBatch(
                new ArrayList<>(submission.records),
                Collections.singletonList(submission),
                new byte[0]);
    }

    // -------------------------------------------------------------------------
    // Worker thread
    // -------------------------------------------------------------------------

    private void runWorker() {
        while (true) {
            ClosableBlockingQueue.PollResult<DeliveryBatch> polled;
            try {
                polled = uploadQueue.poll();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            if (polled.isClosed()) {
                break;
            }
            DeliveryBatch batch = polled.item();
            try {
                processBatch(batch);
            } catch (Throwable e) {
                LOG.log(Level.SEVERE, "Uncaught worker exception while processing stream-load batch", e);
                statBatchesFailed.incrementAndGet();
                Instant now = Instant.now();
                completeBatch(batch, DeliveryResult.failure(asException(e), 0, 0, now, now));
            }
        }
    }

    private void processBatch(DeliveryBatch batch) {
        Instant startTime = Instant.now();
        String label = generateLabel();

        if (config.isFakeSend()) {
            try {
                Thread.sleep(config.getFakeSendDelay().toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            boolean succeed = ThreadLocalRandom.current().nextDouble() < config.getFakeSendSuccessRate();
            statBatches.incrementAndGet();
            if (succeed) {
                statBytesSent.addAndGet(batch.payload.length);
                statRecordsSent.addAndGet(batch.records.size());
                StreamLoadResponse response = StreamLoadResponse.fakeSuccess(
                        label,
                        batch.records.size(),
                        batch.payload.length,
                        config.getFakeSendDelay().toMillis());
                completeBatch(batch, DeliveryResult.success(1, 200, response, startTime, Instant.now()));
            } else {
                statBatchesFailed.incrementAndGet();
                completeBatch(batch, DeliveryResult.failure(
                        new IOException("fake send failure (fakeSendSuccessRate="
                                + config.getFakeSendSuccessRate() + ")"),
                        1, 500, startTime, Instant.now()));
            }
            return;
        }

        int attempts = 0;
        Exception lastError = null;
        int lastHttpStatus = 0;
        StreamLoadResponse lastResponse = null;
        Instant retryDeadline = null;

        while (true) {
            if (retryDeadline != null && !Instant.now().isBefore(retryDeadline)) {
                lastError = new IOException("Upload did not conclude within uploadTimeout="
                        + config.getUploadTimeout());
                break;
            }
            attempts++;

            // Count bytes and records per attempt (matches Go's totalBytesSent / totalRecordsSent semantics)
            statBytesSent.addAndGet(batch.payload.length);
            statRecordsSent.addAndGet(batch.records.size());

            try {
                HttpResponse<String> httpResp = doHttpPut(config.getStreamLoadUrl(), batch.payload, label);
                lastHttpStatus = httpResp.statusCode();

                if (lastHttpStatus >= 200 && lastHttpStatus < 300) {
                    try {
                        lastResponse = parseResponse(httpResp.body());
                    } catch (IOException e) {
                        PollOutcome pollOutcome = pollLabelUntilConclusion(label, startTime, attempts);
                        if (pollOutcome.result != null) {
                            statBatches.incrementAndGet();
                            completeBatch(batch, pollOutcome.result);
                            return;
                        }
                        lastError = pollOutcome.error;
                        if (pollOutcome.retriable) {
                            label = generateLabel();
                            lastResponse = null;
                        } else {
                            break;
                        }
                    }
                    if (lastResponse == null) {
                        // Polling concluded retrying with a fresh label is safe; continue loop.
                    } else if (lastResponse.isSuccess()) {
                        DeliveryResult result = DeliveryResult.success(
                                attempts, lastHttpStatus, lastResponse, startTime, Instant.now());
                        statBatches.incrementAndGet();
                        completeBatch(batch, result);
                        return;
                    } else if (isLabelExistsRunning(lastResponse)) {
                        // Label is already running on Doris; poll for its outcome.
                        PollOutcome pollOutcome = pollLabelUntilConclusion(label, startTime, attempts);
                        if (pollOutcome.result != null) {
                            statBatches.incrementAndGet();
                            completeBatch(batch, pollOutcome.result);
                            return;
                        }
                        lastError = pollOutcome.error;
                        if (pollOutcome.retriable) {
                            label = generateLabel();
                            lastResponse = null;
                        } else {
                            break;
                        }
                    } else {
                        lastError = new IOException("Doris rejected batch: " + lastResponse.getMessage()
                                + " (status=" + lastResponse.getStatus() + ")");
                        break;
                    }
                } else if (isRetriableHttpStatus(lastHttpStatus)) {
                    lastError = new IOException("Retriable HTTP " + lastHttpStatus);
                } else {
                    lastError = new IOException("Non-retriable HTTP " + lastHttpStatus);
                    break;
                }
            } catch (Exception e) {
                lastError = e;
                if (isAmbiguousTransportError(e)) {
                    PollOutcome pollOutcome = pollLabelUntilConclusion(label, startTime, attempts);
                    if (pollOutcome.result != null) {
                        statBatches.incrementAndGet();
                        completeBatch(batch, pollOutcome.result);
                        return;
                    }
                    lastError = pollOutcome.error;
                    if (pollOutcome.retriable) {
                        label = generateLabel();
                    } else {
                        break;
                    }
                } else if (!isRetriableTransportError(e)) {
                    break;
                }
            }

            if (retryDeadline == null) {
                retryDeadline = Instant.now().plus(config.getUploadTimeout());
            }
            if (!Instant.now().isBefore(retryDeadline)) {
                lastError = new IOException("Upload did not conclude within uploadTimeout="
                        + config.getUploadTimeout());
                break;
            }

            long delayMs = retryBackoffMs(attempts);
            long remainingMs = Duration.between(Instant.now(), retryDeadline).toMillis();
            if (remainingMs <= 0 || delayMs >= remainingMs) {
                lastError = new IOException("Upload did not conclude within uploadTimeout="
                        + config.getUploadTimeout());
                break;
            }
            statRetries.incrementAndGet();
            try {
                Thread.sleep(delayMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                lastError = e;
                break;
            }
        }

        statBatchesFailed.incrementAndGet();
        Exception err = lastError != null ? lastError
                : new IOException("Stream load failed after " + attempts + " attempt(s)");
        completeBatch(batch, DeliveryResult.failure(err, attempts, lastHttpStatus, startTime, Instant.now()));
    }

    // -------------------------------------------------------------------------
    // HTTP
    // -------------------------------------------------------------------------

    private PollOutcome pollLabelUntilConclusion(String label, Instant started, int attempts) {
        Instant deadline = Instant.now().plus(config.getStatusPollTimeout());
        long backoffMs = 500;
        LoadStateResponse lastState = null;
        Exception lastError = null;

        while (true) {
            try {
                lastState = doPollLabel(label);
                String state = lastState.state == null ? "" : lastState.state.trim().toUpperCase();
                switch (state) {
                    case "VISIBLE":
                    case "COMMITTED": {
                        StreamLoadResponse response = StreamLoadResponse.fakeSuccess(
                                label, 0, 0, 0);
                        return PollOutcome.success(DeliveryResult.success(
                                attempts, lastState.statusCode, response, started, Instant.now()));
                    }
                    case "ABORTED":
                        return PollOutcome.retry(new IOException(
                                "Load label " + label + " concluded as ABORTED"));
                    case "UNKNOWN":
                        return PollOutcome.retry(new IOException(
                                "Load label " + label + " not found in Doris (state=UNKNOWN)"));
                    case "PREPARE":
                    case "PRECOMMITTED":
                    case "":
                        break;
                    default:
                        lastError = new IOException("Unknown load label state for "
                                + label + ": " + lastState.state);
                        break;
                }
            } catch (Exception e) {
                lastError = e;
            }

            if (!Instant.now().isBefore(deadline)) {
                String last = lastState == null ? "unknown" : lastState.state;
                IOException error = new IOException(
                        "Load label " + label + " did not reach a terminal state before statusPollTimeout="
                                + config.getStatusPollTimeout() + "; last state=" + last,
                        lastError);
                return PollOutcome.fail(error);
            }

            long remainingMs = Duration.between(Instant.now(), deadline).toMillis();
            long sleepMs = Math.min(backoffMs, Math.max(1, remainingMs));
            try {
                Thread.sleep(sleepMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return PollOutcome.fail(e);
            }
            backoffMs = Math.min(backoffMs * 2, 4_000L);
        }
    }

    private LoadStateResponse doPollLabel(String label) throws IOException, InterruptedException {
        String url = buildLoadStateUrl(label);
        HttpResponse<String> response = doHttpGet(url);
        JsonNode node = objectMapper.readTree(response.body());
        String state = node.has("data") && !node.get("data").isNull()
                ? node.get("data").asText()
                : "";
        if (!state.isBlank()) {
            return new LoadStateResponse(response.statusCode(), state, textField(node, "msg"));
        }
        throw new IOException("Load state request failed: msg=" + textField(node, "msg")
                + " code=" + textField(node, "code"));
    }

    private String buildLoadStateUrl(String label) {
        URI base = URI.create(config.getConfiguredStreamLoadUrl() != null
                ? config.getConfiguredStreamLoadUrl()
                : config.getEndpoint());
        String database = pollDatabase();
        String path = "/api/" + database + "/get_load_state";
        String query = "label=" + URLEncoder.encode(label, StandardCharsets.UTF_8);
        return base.resolve(path + "?" + query).toString();
    }

    private String pollDatabase() {
        if (config.getDatabase() != null && !config.getDatabase().isBlank()) {
            return config.getDatabase();
        }
        String streamLoadUrl = config.getConfiguredStreamLoadUrl();
        if (streamLoadUrl == null || streamLoadUrl.isBlank()) {
            return "";
        }
        String[] parts = URI.create(streamLoadUrl).getPath().split("/");
        for (int i = 0; i + 3 < parts.length; i++) {
            if ("api".equals(parts[i]) && "_stream_load".equals(parts[i + 3])) {
                return parts[i + 1];
            }
        }
        return "";
    }

    /** Executes an HTTP GET with redirect following (up to MAX_REDIRECTS hops). */
    private HttpResponse<String> doHttpGet(String url) throws IOException, InterruptedException {
        int redirects = 0;
        String currentUrl = url;
        while (redirects <= MAX_REDIRECTS) {
            HttpRequest.Builder req = HttpRequest.newBuilder()
                    .uri(URI.create(currentUrl))
                    .timeout(config.getUploadRequestTimeout())
                    .GET();
            boolean includeSensitiveHeaders = redirects <= 1;
            addAuthHeader(req, includeSensitiveHeaders);
            addCustomHeaders(req, includeSensitiveHeaders);
            HttpResponse<String> response = httpClient.send(
                    req.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            int status = response.statusCode();
            if (status == 301 || status == 302 || status == 303
                    || status == 307 || status == 308) {
                String location = response.headers().firstValue("Location")
                        .orElseThrow(() -> new IOException("Redirect " + status + " without Location header"));
                currentUrl = resolveUrl(currentUrl, location);
                redirects++;
                LOG.fine("Poll redirect " + redirects + " -> " + currentUrl);
                continue;
            }
            return response;
        }
        throw new IOException("Exceeded " + MAX_REDIRECTS + " redirects for " + url);
    }

    private HttpResponse<String> doHttpPut(String url, byte[] payload, String label)
            throws IOException, InterruptedException {
        int redirects = 0;
        String currentUrl = url;

        while (redirects <= MAX_REDIRECTS) {
            HttpRequest request = buildRequest(currentUrl, payload, label, redirects <= 1);
            HttpResponse<String> response = httpClient.send(
                    request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));

            int status = response.statusCode();
            if (status == 301 || status == 302 || status == 303
                    || status == 307 || status == 308) {
                String location = response.headers().firstValue("Location")
                        .orElseThrow(() -> new IOException("Redirect " + status + " without Location header"));
                currentUrl = resolveUrl(currentUrl, location);
                redirects++;
                LOG.fine("Following redirect " + redirects + " -> " + currentUrl);
                continue;
            }
            return response;
        }
        throw new IOException("Exceeded " + MAX_REDIRECTS + " redirects for " + url);
    }

    private HttpRequest buildRequest(String url, byte[] payload, String label, boolean includeSensitiveHeaders) {
        HttpRequest.Builder req = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(config.getUploadRequestTimeout())
                .expectContinue(true)
                .PUT(HttpRequest.BodyPublishers.ofByteArray(payload));

        addAuthHeader(req, includeSensitiveHeaders);
        req.header("label", label);

        if (config.getMode() == Mode.JSON) {
            req.header("Content-Type", "application/json");
            req.header("format", "json");
            req.header("strip_outer_array", "true");
            req.header("read_json_by_line", "false");
        } else {
            req.header("Content-Type", "text/csv");
            req.header("format", "csv");
            req.header("column_separator", config.getCsvSeparator());
            if (config.getCsvEnclose() != null && !config.getCsvEnclose().isEmpty()) {
                req.header("enclose", config.getCsvEnclose());
            }
        }

        if (!config.getColumns().isEmpty()) {
            req.header("columns", String.join(",", config.getColumns()));
        }

        addCustomHeaders(req, includeSensitiveHeaders);

        return req.build();
    }

    private void addAuthHeader(HttpRequest.Builder req, boolean includeSensitiveHeaders) {
        if (!includeSensitiveHeaders) {
            return;
        }
        if (config.getUsername() != null && !config.getUsername().isEmpty()) {
            String creds = config.getUsername() + ":" + config.getPassword();
            String encoded = Base64.getEncoder()
                    .encodeToString(creds.getBytes(StandardCharsets.UTF_8));
            req.header("Authorization", "Basic " + encoded);
        }
    }

    private void addCustomHeaders(HttpRequest.Builder req, boolean includeSensitiveHeaders) {
        if (!includeSensitiveHeaders) {
            return;
        }
        for (Map.Entry<String, List<String>> entry : config.getCustomHeaders().entrySet()) {
            for (String value : entry.getValue()) {
                req.header(entry.getKey(), value);
            }
        }
    }

    private StreamLoadResponse parseResponse(String body) throws IOException {
        try {
            return objectMapper.readValue(body, StreamLoadResponse.class);
        } catch (Exception e) {
            throw new IOException("Cannot parse Doris response: " + body, e);
        }
    }

    private static String textField(JsonNode node, String field) {
        if (node == null || !node.has(field) || node.get(field).isNull()) {
            return "";
        }
        return node.get(field).asText();
    }

    private static boolean isRetriableHttpStatus(int status) {
        return status == 408 || status == 429 || status >= 500;
    }

    private static boolean isRetriableTransportError(Exception e) {
        // Dial-level failures: safe to retry with same label (no data was sent)
        return e instanceof ConnectException || e instanceof UnknownHostException;
    }

    private static boolean isAmbiguousTransportError(Exception e) {
        // Timeout or mid-transfer IO error: data may or may not have landed; must poll.
        if (e instanceof UnknownHostException) return false;
        return e instanceof HttpTimeoutException
                || e instanceof IOException && !(e instanceof ConnectException);
    }

    private static boolean isLabelExistsRunning(StreamLoadResponse r) {
        return "label already exists".equalsIgnoreCase(r.getStatus())
                && "running".equalsIgnoreCase(r.getExistingJobStatus());
    }

    private static long retryBackoffMs(int attempt) {
        long delay = 1_000L;
        for (int i = 1; i < attempt; i++) {
            delay = Math.min(delay * 2, 4_000L);
        }
        return delay;
    }

    private static String resolveUrl(String base, String location) {
        if (location.startsWith("http://") || location.startsWith("https://")) {
            return location;
        }
        return URI.create(base).resolve(location).toString();
    }

    private static Exception asException(Throwable throwable) {
        return throwable instanceof Exception
                ? (Exception) throwable
                : new RuntimeException(throwable);
    }

    // -------------------------------------------------------------------------
    // Completion
    // -------------------------------------------------------------------------

    private void completeBatch(DeliveryBatch batch, DeliveryResult result) {
        if (!result.isSuccess()) {
            statRecordsFailed.addAndGet(batch.records.size());
        }
        for (PendingSubmission submission : batch.submissions) {
            submission.future.complete(result);
        }
        for (PendingSubmission submission : batch.submissions) {
            invokeCallback(submission, result);
        }
    }

    private void invokeCallback(PendingSubmission submission, DeliveryResult result) {
        if (submission.callback != null) {
            Instant started = Instant.now();
            try {
                submission.callback.accept(result);
            } catch (Throwable e) {
                LOG.log(Level.WARNING, "Delivery callback threw an exception", e);
            } finally {
                long elapsedMs = Duration.between(started, Instant.now()).toMillis();
                if (elapsedMs > config.getSlowCallbackWarn().toMillis()) {
                    LOG.log(Level.INFO, "Delivery callback took {0} ms", elapsedMs);
                }
            }
        }
    }

    // -------------------------------------------------------------------------
    // Misc helpers
    // -------------------------------------------------------------------------

    private String generateLabel() {
        long ts = System.currentTimeMillis();
        // Full 64-bit random value: about 62 bits of entropy (matches Go's label entropy).
        long rnd = RANDOM.nextLong();
        return config.getLabelPrefix() + "_" + ts + "_" + Long.toHexString(rnd);
    }

    private static ThreadFactory daemonThreadFactory(String namePrefix) {
        AtomicLong threadNumber = new AtomicLong();
        return task -> {
            Thread thread = new Thread(task, namePrefix + "-" + threadNumber.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
    }

    private static void joinUninterruptibly(Thread thread) {
        boolean interrupted = false;
        while (true) {
            try {
                thread.join();
                break;
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private static void awaitTerminationUninterruptibly(ExecutorService executor) {
        boolean interrupted = false;
        while (true) {
            try {
                if (executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS)) {
                    break;
                }
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private HttpClient buildHttpClient() {
        if (config.getHttpClient() != null
                && !config.isTlsSkipVerify()
                && (config.getTlsCaCertPath() == null || config.getTlsCaCertPath().isBlank())) {
            return config.getHttpClient();
        }

        HttpClient.Builder b = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(30))
                .followRedirects(HttpClient.Redirect.NEVER);

        if (config.isTlsSkipVerify()) {
            try {
                SSLContext ctx = SSLContext.getInstance("TLS");
                ctx.init(null, new TrustManager[]{new X509TrustManager() {
                    public void checkClientTrusted(X509Certificate[] c, String a) {}
                    public void checkServerTrusted(X509Certificate[] c, String a) {}
                    public X509Certificate[] getAcceptedIssuers() { return new X509Certificate[0]; }
                }}, RANDOM);
                b.sslContext(ctx);
            } catch (Exception e) {
                throw new RuntimeException("Failed to create TLS-skip-verify SSLContext", e);
            }
        } else if (config.getTlsCaCertPath() != null && !config.getTlsCaCertPath().isBlank()) {
            try {
                b.sslContext(buildCaSslContext(config.getTlsCaCertPath()));
            } catch (Exception e) {
                throw new RuntimeException(
                        "Failed to load CA cert from " + config.getTlsCaCertPath(), e);
            }
        }

        return b.build();
    }

    /**
     * Builds an SSLContext that trusts the given PEM or PKCS12 CA certificate file
     * in addition to the JVM default trust store.
     */
    private SSLContext buildCaSslContext(String caCertPath) throws Exception {
        KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
        try (InputStream defaults = Files.newInputStream(Path.of(System.getProperty("java.home"),
                "lib", "security", "cacerts"))) {
            ks.load(defaults, "changeit".toCharArray());
        } catch (Exception e) {
            ks.load(null, null);
        }

        String lower = caCertPath.toLowerCase();
        if (lower.endsWith(".p12") || lower.endsWith(".pfx")) {
            try (InputStream is = Files.newInputStream(Path.of(caCertPath))) {
                ks.load(is, null);
            }
        } else {
            // Treat as PEM X.509 certificate
            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            try (InputStream is = Files.newInputStream(Path.of(caCertPath))) {
                int i = 0;
                for (java.security.cert.Certificate cert : cf.generateCertificates(is)) {
                    ks.setCertificateEntry("custom-ca-" + i++, cert);
                }
            }
        }

        TrustManagerFactory tmf = TrustManagerFactory.getInstance(
                TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(ks);
        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(null, tmf.getTrustManagers(), RANDOM);
        return ctx;
    }

    // -------------------------------------------------------------------------
    // Internal data structures
    // -------------------------------------------------------------------------

    static class PendingRecord {
        final byte[] data;
        final CompletableFuture<DeliveryResult> future;

        PendingRecord(byte[] data, CompletableFuture<DeliveryResult> future) {
            this.data = data;
            this.future = future;
        }
    }

    static class PendingSubmission {
        final List<PendingRecord> records;
        final CompletableFuture<DeliveryResult> future;
        final Consumer<DeliveryResult> callback;
        final int byteSize;

        PendingSubmission(List<PendingRecord> records,
                          CompletableFuture<DeliveryResult> future,
                          Consumer<DeliveryResult> callback,
                          int byteSize) {
            this.records = records;
            this.future = future;
            this.callback = callback;
            this.byteSize = byteSize;
        }
    }

    static class DeliveryBatch {
        final List<PendingRecord> records;
        final List<PendingSubmission> submissions;
        final byte[] payload;

        DeliveryBatch(List<PendingRecord> records, List<PendingSubmission> submissions, byte[] payload) {
            this.records = records;
            this.submissions = submissions;
            this.payload = payload;
        }
    }

    static class LoadStateResponse {
        final int statusCode;
        final String state;
        final String message;

        LoadStateResponse(int statusCode, String state, String message) {
            this.statusCode = statusCode;
            this.state = state;
            this.message = message;
        }
    }

    static class PollOutcome {
        final DeliveryResult result;
        final Exception error;
        final boolean retriable;

        private PollOutcome(DeliveryResult result, Exception error, boolean retriable) {
            this.result = result;
            this.error = error;
            this.retriable = retriable;
        }

        static PollOutcome success(DeliveryResult result) {
            return new PollOutcome(result, null, false);
        }

        static PollOutcome retry(Exception error) {
            return new PollOutcome(null, error, true);
        }

        static PollOutcome fail(Exception error) {
            return new PollOutcome(null, error, false);
        }
    }
}
