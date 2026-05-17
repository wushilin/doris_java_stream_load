package net.wushilin.doris;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;

class StreamLoadPerfTest {

    private static final int DEFAULT_MESSAGES = 20_000_000;
    private static final Duration DEFAULT_WAIT_TIMEOUT = Duration.ofMinutes(10);
    private static final int SUBMITTER_THREADS = 10;

    @Test
    @EnabledIfSystemProperty(named = "streamload.perf", matches = "true")
    void closableBlockingQueueTenProducersOneConsumer() throws Exception {
        int producers = Integer.getInteger("streamload.perf.queue.producers", 10);
        int messages = Integer.getInteger("streamload.perf.queue.messages", 5_000_000);
        int capacity = Integer.getInteger("streamload.perf.queue.capacity", 100_000);

        ClosableBlockingQueue<Integer> queue = new ClosableBlockingQueue<>(capacity);
        CountDownLatch start = new CountDownLatch(1);
        AtomicLong consumed = new AtomicLong();

        Thread consumer = new Thread(() -> {
            try {
                start.await();
                while (true) {
                    ClosableBlockingQueue.PollResult<Integer> item = queue.poll();
                    if (item.isClosed()) {
                        break;
                    }
                    consumed.incrementAndGet();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "queue-consumer");

        int base = messages / producers;
        Thread[] producerThreads = new Thread[producers];
        AtomicLong enqueueNanos = new AtomicLong();
        for (int p = 0; p < producers; p++) {
            final int idx = p;
            producerThreads[p] = new Thread(() -> {
                int startIndex = idx * base;
                int endIndex = idx == producers - 1 ? messages : startIndex + base;
                try {
                    start.await();
                    for (int i = startIndex; i < endIndex; i++) {
                        long enqueueStarted = System.nanoTime();
                        if (!queue.put(i).ok()) {
                            throw new IllegalStateException("queue closed before benchmark finished");
                        }
                        enqueueNanos.addAndGet(System.nanoTime() - enqueueStarted);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }, "queue-producer-" + p);
        }

        consumer.start();
        for (Thread producerThread : producerThreads) {
            producerThread.start();
        }

        long started = System.nanoTime();
        start.countDown();
        for (Thread producerThread : producerThreads) {
            producerThread.join();
        }
        long producersDone = System.nanoTime();
        queue.close();
        consumer.join();
        long finished = System.nanoTime();

        assertEquals(messages, consumed.get());

        double enqueueSeconds = (producersDone - started) / 1_000_000_000.0;
        double totalSeconds = (finished - started) / 1_000_000_000.0;
        double averageEnqueueNanos = enqueueNanos.get() / (double) messages;

        System.out.printf(Locale.ROOT,
                "ClosableBlockingQueue: producers=%d consumers=1 messages=%d capacity=%d "
                        + "enqueue_elapsed=%.3fs total_elapsed=%.3fs enqueue_ops_per_second=%.2f "
                        + "dequeue_ops_per_second=%.2f avg_enqueue_wait_ns=%.1f%n",
                producers,
                messages,
                capacity,
                enqueueSeconds,
                totalSeconds,
                messages / enqueueSeconds,
                consumed.get() / totalSeconds,
                averageEnqueueNanos);
    }

    @Test
    @EnabledIfSystemProperty(named = "streamload.perf", matches = "true")
    void fakeSendTwentyMillionCsvAndJsonMessages() throws Exception {
        int messages = Integer.getInteger("streamload.perf.messages", DEFAULT_MESSAGES);
        Duration waitTimeout = Duration.ofSeconds(
                Long.getLong("streamload.perf.wait.seconds", DEFAULT_WAIT_TIMEOUT.toSeconds()));

        PerfResult csv = runFakeSendPerf(Mode.CSV, messages, waitTimeout);
        PerfResult json = runFakeSendPerf(Mode.JSON, messages, waitTimeout);

        writeReport(csv, json);

        assertEquals(messages, csv.recordsSent);
        assertEquals(messages, json.recordsSent);
    }

    private PerfResult runFakeSendPerf(Mode mode, int messages, Duration waitTimeout) throws Exception {
        StreamLoadConfig config = StreamLoadConfig.builder()
                .endpoint("http://localhost:8030")
                .database("perf")
                .table("events")
                .mode(mode)
                .columns("event_time", "user_id", "event_name")
                .validationMode(ValidationMode.NONE)
                .maxQueueSize(1_000_000)
                .batchBytes(90L * 1024 * 1024)
                .linger(Duration.ofMillis(5))
                .uploadWorkers(Runtime.getRuntime().availableProcessors())
                .fakeSend(true)
                .fakeSendDelay(Duration.ZERO)
                .build();

        String record = mode == Mode.CSV
                ? "2026-05-01T10:00:00Z,1001,login"
                : "{\"event_time\":\"2026-05-01T10:00:00Z\",\"user_id\":1001,\"event_name\":\"login\"}";

        AtomicLong completedCounter = new AtomicLong(0);
        LinkedBlockingQueue<StreamLoadHandle> handleQueue = new LinkedBlockingQueue<>();

        Instant started = Instant.now();
        try (StreamLoadClient client = new StreamLoadClient(config)) {

            // Reaper knows exactly `messages` handles will arrive — take() each and get() the future.
            Thread reaper = new Thread(() -> {
                for (int i = 0; i < messages; i++) {
                    try {
                        handleQueue.take().resultFuture().get();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    } catch (Exception ignored) {}
                }
            }, "reaper");
            reaper.start();

            int base = messages / SUBMITTER_THREADS;
            Thread[] submitters = new Thread[SUBMITTER_THREADS];
            for (int t = 0; t < SUBMITTER_THREADS; t++) {
                final int idx = t;
                submitters[t] = new Thread(() -> {
                    int start = idx * base;
                    int end = idx == SUBMITTER_THREADS - 1 ? messages : start + base;
                    for (int i = start; i < end; i++) {
                        try {
                            StreamLoadHandle handle = client.submitWithCallback(
                                    record, result -> completedCounter.incrementAndGet());
                            handleQueue.add(handle);
                        } catch (StreamLoadException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }, "submitter-" + t);
                submitters[t].start();
            }

            for (Thread t : submitters) {
                t.join();
            }
            reaper.join(waitTimeout.toMillis());

            ClientStats stats = client.stats();
            Duration elapsed = Duration.between(started, Instant.now());
            return new PerfResult(mode, messages, stats.getTotalRecordsSent(),
                    stats.getTotalBatches(), stats.getTotalBytesSent(), elapsed);
        }
    }

    private void writeReport(PerfResult csv, PerfResult json) throws IOException {
        Path output = Path.of("build", "reports", "perf", "stream-load-perf.txt");
        Files.createDirectories(output.getParent());
        String report = """
                Doris Java Stream Load fake-send perf
                messages_per_mode=%d

                %s
                %s
                """.formatted(csv.messages, csv.format(), json.format());
        Files.writeString(output, report, StandardCharsets.UTF_8);
        System.out.print(report);
    }

    private static class PerfResult {
        final Mode mode;
        final int messages;
        final long recordsSent;
        final long batches;
        final long bytesSent;
        final Duration elapsed;

        PerfResult(Mode mode, int messages, long recordsSent, long batches,
                   long bytesSent, Duration elapsed) {
            this.mode = mode;
            this.messages = messages;
            this.recordsSent = recordsSent;
            this.batches = batches;
            this.bytesSent = bytesSent;
            this.elapsed = elapsed;
        }

        String format() {
            double seconds = elapsed.toNanos() / 1_000_000_000.0;
            double recordsPerSecond = seconds == 0 ? 0 : recordsSent / seconds;
            double mbPerSecond = seconds == 0 ? 0 : (bytesSent / 1024.0 / 1024.0) / seconds;
            return String.format(Locale.ROOT,
                    "%s: elapsed=%.3fs records=%d batches=%d records_per_second=%.2f bytes=%d MiB_per_second=%.2f",
                    mode, seconds, recordsSent, batches, recordsPerSecond, bytesSent, mbPerSecond);
        }
    }
}
