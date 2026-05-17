package net.wushilin.doris;

import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpServer;
import java.net.http.HttpClient;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class StreamLoadClientTest {

    private StreamLoadConfig fakeCsvConfig() {
        return StreamLoadConfig.builder()
                .endpoint("http://localhost:8030")
                .database("testdb")
                .table("testtable")
                .mode(Mode.CSV)
                .columns("id", "name", "age")
                .csvSeparator(",")
                .fakeSend(true)
                .fakeSendDelay(Duration.ofMillis(10))
                .linger(Duration.ofMillis(50))
                .build();
    }

    private StreamLoadConfig fakeJsonConfig() {
        return StreamLoadConfig.builder()
                .endpoint("http://localhost:8030")
                .database("testdb")
                .table("testtable")
                .mode(Mode.JSON)
                .columns("id", "name")
                .fakeSend(true)
                .fakeSendDelay(Duration.ofMillis(10))
                .linger(Duration.ofMillis(50))
                .build();
    }

    @Test
    void sendSingleCsvRecord() throws Exception {
        try (StreamLoadClient client = new StreamLoadClient(fakeCsvConfig())) {
            StreamLoadHandle handle = client.submit("1,Alice,30");
            assertEquals(1, handle.getRecordCount());
            DeliveryResult result = handle.waitForResult(Duration.ofSeconds(5));
            assertTrue(result.isSuccess());
            assertEquals(1, result.getAttempts());
            assertNotNull(result.getResponse());
            assertNotNull(result.getResponse().getLabel());
        }
    }

    @Test
    void sendSingleJsonRecord() throws Exception {
        try (StreamLoadClient client = new StreamLoadClient(fakeJsonConfig())) {
            StreamLoadHandle handle = client.submit("{\"id\":1,\"name\":\"Alice\"}");
            DeliveryResult result = handle.waitForResult(Duration.ofSeconds(5));
            assertTrue(result.isSuccess());
        }
    }

    @Test
    void rejectInvalidJsonInSyntaxMode() {
        StreamLoadConfig cfg = StreamLoadConfig.builder()
                .endpoint("http://localhost:8030")
                .database("testdb")
                .table("testtable")
                .mode(Mode.JSON)
                .columns("id")
                .validationMode(ValidationMode.SYNTAX)
                .fakeSend(true)
                .build();

        try (StreamLoadClient client = new StreamLoadClient(cfg)) {
            StreamLoadException ex = assertThrows(StreamLoadException.class,
                    () -> client.submit("not valid json"));
            assertEquals(StreamLoadException.ErrorCode.INVALID_RECORD, ex.getCode());
        }
    }

    @Test
    void rejectMissingColumnInStrictMode() {
        StreamLoadConfig cfg = StreamLoadConfig.builder()
                .endpoint("http://localhost:8030")
                .database("testdb")
                .table("testtable")
                .mode(Mode.JSON)
                .columns("id", "name", "age")
                .validationMode(ValidationMode.STRICT)
                .fakeSend(true)
                .build();

        try (StreamLoadClient client = new StreamLoadClient(cfg)) {
            // missing "age"
            StreamLoadException ex = assertThrows(StreamLoadException.class,
                    () -> client.submit("{\"id\":1,\"name\":\"Alice\"}"));
            assertEquals(StreamLoadException.ErrorCode.INVALID_RECORD, ex.getCode());
        }
    }

    @Test
    void syntaxJsonAllowsMissingConfiguredColumns() throws Exception {
        StreamLoadConfig cfg = StreamLoadConfig.builder()
                .endpoint("http://localhost:8030")
                .database("testdb")
                .table("testtable")
                .mode(Mode.JSON)
                .columns("id", "name", "age")
                .validationMode(ValidationMode.SYNTAX)
                .fakeSend(true)
                .fakeSendDelay(Duration.ofMillis(1))
                .build();

        try (StreamLoadClient client = new StreamLoadClient(cfg)) {
            DeliveryResult result = client.submit("{\"id\":1,\"name\":\"Alice\"}")
                    .waitForResult(Duration.ofSeconds(5));
            assertTrue(result.isSuccess());
        }
    }

    @Test
    void csvValidationRejectsMultipleRows() {
        StreamLoadConfig cfg = StreamLoadConfig.builder()
                .endpoint("http://localhost:8030")
                .database("testdb")
                .table("testtable")
                .mode(Mode.CSV)
                .columns("id", "name")
                .fakeSend(true)
                .build();

        try (StreamLoadClient client = new StreamLoadClient(cfg)) {
            StreamLoadException ex = assertThrows(StreamLoadException.class,
                    () -> client.submit("1,Alice\n2,Bob"));
            assertEquals(StreamLoadException.ErrorCode.INVALID_RECORD, ex.getCode());
        }
    }

    @Test
    void csvValidationHandlesEscapedQuotes() throws Exception {
        StreamLoadConfig cfg = StreamLoadConfig.builder()
                .endpoint("http://localhost:8030")
                .database("testdb")
                .table("testtable")
                .mode(Mode.CSV)
                .columns("id", "name")
                .fakeSend(true)
                .fakeSendDelay(Duration.ofMillis(1))
                .build();

        try (StreamLoadClient client = new StreamLoadClient(cfg)) {
            DeliveryResult result = client.submit("1,\"Alice \"\"A\"\"\"")
                    .waitForResult(Duration.ofSeconds(5));
            assertTrue(result.isSuccess());
        }
    }

    @Test
    void csvValidationUsesConfiguredSeparatorAndQuote() throws Exception {
        StreamLoadConfig cfg = StreamLoadConfig.builder()
                .endpoint("http://localhost:8030")
                .database("testdb")
                .table("testtable")
                .mode(Mode.CSV)
                .columns("id", "name")
                .csvSeparator("|")
                .csvQuote("'")
                .fakeSend(true)
                .fakeSendDelay(Duration.ofMillis(1))
                .build();

        try (StreamLoadClient client = new StreamLoadClient(cfg)) {
            DeliveryResult result = client.submit("1|'Alice ''A'''")
                    .waitForResult(Duration.ofSeconds(5));
            assertTrue(result.isSuccess());

            StreamLoadException ex = assertThrows(StreamLoadException.class,
                    () -> client.submit("1,Alice"));
            assertEquals(StreamLoadException.ErrorCode.INVALID_RECORD, ex.getCode());
        }
    }

    @Test
    void rejectNullRecordWithStreamLoadException() {
        try (StreamLoadClient client = new StreamLoadClient(fakeCsvConfig())) {
            StreamLoadException ex = assertThrows(StreamLoadException.class,
                    () -> client.submit(null));
            assertEquals(StreamLoadException.ErrorCode.INVALID_RECORD, ex.getCode());
        }
    }

    @Test
    void sendBatchOfRecords() throws Exception {
        try (StreamLoadClient client = new StreamLoadClient(fakeCsvConfig())) {
            List<String> records = List.of("1,Alice,30", "2,Bob,25", "3,Carol,40");
            StreamLoadHandle handle = client.submitBatch(records);
            assertEquals(3, handle.getRecordCount());
            assertTrue(handle.getByteSize() > 0);
            assertTrue(handle.waitForResult(Duration.ofSeconds(5)).isSuccess());
        }
    }

    @Test
    void callbackIsInvokedOncePerSubmission() throws Exception {
        List<DeliveryResult> results = new ArrayList<>();
        try (StreamLoadClient client = new StreamLoadClient(fakeCsvConfig())) {
            StreamLoadHandle handle = client.submitBatchWithCallback(
                    List.of("1,Alice,30", "2,Bob,25"),
                    results::add
            );
            handle.waitForResult(Duration.ofSeconds(5));
        }
        assertEquals(1, results.size());
        assertTrue(results.get(0).isSuccess());
    }

    @Test
    void callbackErrorDoesNotStrandOtherSubmissionsInBatch() throws Exception {
        CountDownLatch secondCallback = new CountDownLatch(1);

        try (StreamLoadClient client = new StreamLoadClient(fakeCsvConfig())) {
            StreamLoadHandle first = client.submitWithCallback("1,Alice,30", result -> {
                throw new AssertionError("boom");
            });
            StreamLoadHandle second = client.submitWithCallback("2,Bob,25", result -> secondCallback.countDown());

            assertTrue(first.waitForResult(Duration.ofSeconds(5)).isSuccess());
            assertTrue(second.waitForResult(Duration.ofSeconds(5)).isSuccess());
            assertTrue(secondCallback.await(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void rejectAfterClose() throws Exception {
        StreamLoadClient client = new StreamLoadClient(fakeCsvConfig());
        client.close();
        StreamLoadException ex = assertThrows(StreamLoadException.class,
                () -> client.submit("1,Alice,30"));
        assertEquals(StreamLoadException.ErrorCode.CLIENT_CLOSED, ex.getCode());
    }

    @Test
    void closeSignalsBatcherLingerAndCompletesAcceptedHandle() throws Exception {
        StreamLoadConfig cfg = StreamLoadConfig.builder()
                .endpoint("http://localhost:8030")
                .database("testdb")
                .table("testtable")
                .mode(Mode.CSV)
                .columns("id", "name", "age")
                .fakeSend(true)
                .fakeSendDelay(Duration.ofMillis(1))
                .linger(Duration.ofSeconds(30))
                .build();

        StreamLoadClient client = new StreamLoadClient(cfg);
        StreamLoadHandle handle = client.submit("1,Alice,30");

        long started = System.nanoTime();
        client.close();
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);

        assertTrue(elapsedMs < 2_000, "close should signal linger promptly, elapsedMs=" + elapsedMs);
        assertTrue(handle.isDone());
        assertTrue(handle.getResultNow().isSuccess());
    }

    @Test
    void closeDrainsPendingSubmissionsFromIntakeQueue() throws Exception {
        StreamLoadConfig cfg = StreamLoadConfig.builder()
                .endpoint("http://localhost:8030")
                .database("testdb")
                .table("testtable")
                .mode(Mode.CSV)
                .columns("id")
                .fakeSend(true)
                .fakeSendDelay(Duration.ofMillis(1))
                .linger(Duration.ofSeconds(30))
                .build();

        List<StreamLoadHandle> handles = new ArrayList<>();
        StreamLoadClient client = new StreamLoadClient(cfg);
        for (int i = 0; i < 20; i++) {
            handles.add(client.submit(Integer.toString(i)));
        }

        client.close();

        for (StreamLoadHandle handle : handles) {
            assertTrue(handle.isDone());
            assertTrue(handle.getResultNow().isSuccess());
        }
    }

    @Test
    void closeWaitsForUploadQueueToDrain() throws Exception {
        StreamLoadConfig cfg = StreamLoadConfig.builder()
                .endpoint("http://localhost:8030")
                .database("testdb")
                .table("testtable")
                .mode(Mode.CSV)
                .columns("id")
                .batchBytes(1)
                .maxUploadQueueSize(1)
                .uploadWorkers(1)
                .fakeSend(true)
                .fakeSendDelay(Duration.ofMillis(25))
                .linger(Duration.ofSeconds(30))
                .build();

        List<StreamLoadHandle> handles = new ArrayList<>();
        StreamLoadClient client = new StreamLoadClient(cfg);
        for (int i = 0; i < 5; i++) {
            handles.add(client.submit(Integer.toString(i)));
        }

        client.close();

        for (StreamLoadHandle handle : handles) {
            assertTrue(handle.isDone());
            assertTrue(handle.getResultNow().isSuccess());
        }
        assertEquals(5, client.stats().getTotalRecordsSent());
    }

    @Test
    void statsAreUpdatedAfterSend() throws Exception {
        try (StreamLoadClient client = new StreamLoadClient(fakeCsvConfig())) {
            client.submit("1,Alice,30").waitForResult(Duration.ofSeconds(5));
            client.submit("2,Bob,25").waitForResult(Duration.ofSeconds(5));

            ClientStats stats = client.stats();
            assertEquals(2, stats.getTotalRecordsSent());
            assertEquals(0, stats.getTotalRecordsFailed());
        }
    }

    @Test
    void handleIsDoneAfterCompletion() throws Exception {
        try (StreamLoadClient client = new StreamLoadClient(fakeCsvConfig())) {
            StreamLoadHandle handle = client.submit("1,Alice,30");
            handle.waitForResult(Duration.ofSeconds(5));
            assertTrue(handle.isDone());
            assertNotNull(handle.getResultNow());
            assertSame(handle.resultFuture(), handle.asFuture());
        }
    }

    @Test
    void submitAsyncCompletesWithAcceptedHandleThenDeliveryResult() throws Exception {
        try (StreamLoadClient client = new StreamLoadClient(fakeCsvConfig())) {
            CompletableFuture<StreamLoadHandle> admission = client.submitBatchAsync(
                    List.of("1,Alice,30", "2,Bob,25"));
            StreamLoadHandle handle = admission.get(5, java.util.concurrent.TimeUnit.SECONDS);
            assertEquals(2, handle.getRecordCount());
            DeliveryResult result = handle.resultFuture().get(5, java.util.concurrent.TimeUnit.SECONDS);
            assertTrue(result.isSuccess());
        }
    }

    @Test
    void queueFullExceptionHasCorrectErrorCode() throws Exception {
        // Verify the QUEUE_FULL path by filling the upload queue so the batcher
        // blocks and the intake queue backs up.
        // Strategy: batchBytes=1 forces every record into its own batch;
        // uploadWorkers=1 with a long delay fills the upload queue;
        // once the batcher blocks on uploadQueue.put(), the intake queue fills.
        StreamLoadConfig cfg = StreamLoadConfig.builder()
                .endpoint("http://localhost:8030")
                .database("testdb")
                .table("testtable")
                .mode(Mode.CSV)
                .columns("id")
                .maxQueueSize(4)           // small intake queue
                .batchBytes(1)             // every record becomes its own batch
                .uploadWorkers(1)
                .maxUploadQueueSize(1)
                .fakeSend(true)
                .fakeSendDelay(Duration.ofSeconds(2))   // worker is blocked long enough to fill queues
                .linger(Duration.ofMillis(1))
                .maxQueueWaitTime(Duration.ofMillis(200))
                .build();

        try (StreamLoadClient client = new StreamLoadClient(cfg)) {
            // Send enough records to saturate the upload queue and pin the batcher
            for (int i = 0; i < 10; i++) {
                try { client.submit("a"); } catch (StreamLoadException ignored) {}
            }

            // Wait until the batcher is blocked (upload queue full) and intake queue fills
            Thread.sleep(300);

            StreamLoadException ex = assertThrows(StreamLoadException.class, () -> {
                for (int i = 0; i < 20; i++) {
                    client.submit("z");
                }
            });
            assertEquals(StreamLoadException.ErrorCode.QUEUE_FULL, ex.getCode());
        }
    }

    @Test
    void concurrentTimedAdmissionWaitsIndependently() throws Exception {
        StreamLoadConfig cfg = StreamLoadConfig.builder()
                .endpoint("http://localhost:8030")
                .database("testdb")
                .table("testtable")
                .mode(Mode.CSV)
                .columns("id")
                .maxQueueSize(1)
                .batchBytes(1)
                .uploadWorkers(1)
                .maxUploadQueueSize(1)
                .fakeSend(true)
                .fakeSendDelay(Duration.ofMillis(250))
                .linger(Duration.ofMillis(1))
                .build();

        try (StreamLoadClient client = new StreamLoadClient(cfg)) {
            for (int i = 0; i < 6; i++) {
                try { client.submit("a", Duration.ofMillis(20)); } catch (StreamLoadException ignored) {}
            }
            Thread.sleep(100);

            CountDownLatch start = new CountDownLatch(1);
            AtomicInteger queueFull = new AtomicInteger();
            List<CompletableFuture<Void>> attempts = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                attempts.add(CompletableFuture.runAsync(() -> {
                    try {
                        start.await();
                        client.submit("z", Duration.ofMillis(120));
                    } catch (StreamLoadException e) {
                        if (e.getCode() == StreamLoadException.ErrorCode.QUEUE_FULL) {
                            queueFull.incrementAndGet();
                        } else {
                            throw new RuntimeException(e);
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }));
            }

            long started = System.nanoTime();
            start.countDown();
            CompletableFuture.allOf(attempts.toArray(new CompletableFuture[0])).get(2, TimeUnit.SECONDS);
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);

            assertTrue(queueFull.get() >= 1, "at least one admission should observe the full queue");
            assertTrue(elapsedMs < 300, "admission attempts should wait in parallel, elapsedMs=" + elapsedMs);
        }
    }

    @Test
    void configValidationRequiresEndpoint() {
        assertThrows(IllegalArgumentException.class, () ->
                StreamLoadConfig.builder()
                        .database("db")
                        .table("t")
                        .columns("id")
                        .build());
    }

    @Test
    void configValidationRequiresDatabase() {
        assertThrows(IllegalArgumentException.class, () ->
                StreamLoadConfig.builder()
                        .endpoint("http://localhost:8030")
                        .table("t")
                        .columns("id")
                        .build());
    }

    @Test
    void configValidationRequiresTable() {
        assertThrows(IllegalArgumentException.class, () ->
                StreamLoadConfig.builder()
                        .endpoint("http://localhost:8030")
                        .database("db")
                        .columns("id")
                        .build());
    }

    @Test
    void streamLoadUrlIsBuiltCorrectly() {
        StreamLoadConfig cfg = StreamLoadConfig.builder()
                .endpoint("http://fe-host:8030")
                .database("mydb")
                .table("orders")
                .columns("id")
                .build();
        assertEquals("http://fe-host:8030/api/mydb/orders/_stream_load", cfg.getStreamLoadUrl());
    }

    @Test
    void configuredStreamLoadUrlIsUsedDirectly() {
        StreamLoadConfig cfg = StreamLoadConfig.builder()
                .streamLoadUrl("http://fe-host:8030/api/mydb/orders/_stream_load")
                .columns("id")
                .build();
        assertEquals("http://fe-host:8030/api/mydb/orders/_stream_load", cfg.getStreamLoadUrl());
    }

    @Test
    void submitBatchUsesOneIntakeQueueSlot() throws Exception {
        StreamLoadConfig cfg = StreamLoadConfig.builder()
                .endpoint("http://localhost:8030")
                .database("testdb")
                .table("testtable")
                .mode(Mode.CSV)
                .columns("id")
                .maxQueueSize(1)
                .maxQueueWaitTime(Duration.ofMillis(100))
                .fakeSend(true)
                .fakeSendDelay(Duration.ofMillis(1))
                .build();

        try (StreamLoadClient client = new StreamLoadClient(cfg)) {
            DeliveryResult result = client.submitBatch(List.of("1", "2", "3"))
                    .waitForResult(Duration.ofSeconds(5));
            assertTrue(result.isSuccess());
        }
    }

    @Test
    void zeroQueueWaitTimeoutEnqueuesWhenCapacityExists() throws Exception {
        StreamLoadConfig cfg = StreamLoadConfig.builder()
                .endpoint("http://localhost:8030")
                .database("testdb")
                .table("testtable")
                .mode(Mode.CSV)
                .columns("id")
                .fakeSend(true)
                .fakeSendDelay(Duration.ofMillis(1))
                .linger(Duration.ofMillis(1))
                .build();

        try (StreamLoadClient client = new StreamLoadClient(cfg)) {
            DeliveryResult result = client.submit("1", Duration.ZERO)
                    .waitForResult(Duration.ofSeconds(5));
            assertTrue(result.isSuccess());
        }
    }

    @Test
    void negativeQueueWaitTimeoutIsRejected() {
        try (StreamLoadClient client = new StreamLoadClient(fakeCsvConfig())) {
            StreamLoadException ex = assertThrows(StreamLoadException.class,
                    () -> client.submit("1,Alice,30", Duration.ofNanos(-1)));
            assertEquals(StreamLoadException.ErrorCode.INVALID_CONFIG, ex.getCode());
        }
    }

    @Test
    void secondRedirectDoesNotForwardAuthOrCustomHeaders() throws Exception {
        AtomicReference<String> finalAuth = new AtomicReference<>();
        AtomicReference<String> finalCustom = new AtomicReference<>();

        HttpServer finalServer = HttpServer.create(new java.net.InetSocketAddress("127.0.0.1", 0), 0);
        finalServer.createContext("/api/db/t/_stream_load", exchange -> {
            finalAuth.set(exchange.getRequestHeaders().getFirst("Authorization"));
            finalCustom.set(exchange.getRequestHeaders().getFirst("X-Secret"));
            byte[] body = "{\"Status\":\"Success\",\"NumberTotalRows\":1,\"NumberLoadedRows\":1}"
                    .getBytes(java.nio.charset.StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        finalServer.start();

        HttpServer firstRedirect = HttpServer.create(new java.net.InetSocketAddress("127.0.0.1", 0), 0);
        firstRedirect.createContext("/api/db/t/_stream_load", exchange -> {
            String location = "http://127.0.0.1:" + finalServer.getAddress().getPort()
                    + "/api/db/t/_stream_load";
            exchange.getResponseHeaders().add("Location", location);
            exchange.sendResponseHeaders(307, -1);
            exchange.close();
        });
        firstRedirect.start();

        HttpServer fe = HttpServer.create(new java.net.InetSocketAddress("127.0.0.1", 0), 0);
        fe.createContext("/api/db/t/_stream_load", exchange -> {
            assertNotNull(exchange.getRequestHeaders().getFirst("Authorization"));
            assertEquals("present", exchange.getRequestHeaders().getFirst("X-Secret"));
            String location = "http://127.0.0.1:" + firstRedirect.getAddress().getPort()
                    + "/api/db/t/_stream_load";
            exchange.getResponseHeaders().add("Location", location);
            exchange.sendResponseHeaders(307, -1);
            exchange.close();
        });
        fe.start();

        StreamLoadConfig cfg = StreamLoadConfig.builder()
                .endpoint("http://127.0.0.1:" + fe.getAddress().getPort())
                .database("db")
                .table("t")
                .mode(Mode.CSV)
                .columns("id")
                .basicAuth("root", "secret")
                .customHeader("X-Secret", "present")
                .linger(Duration.ofMillis(1))
                .build();

        try (StreamLoadClient client = new StreamLoadClient(cfg)) {
            DeliveryResult result = client.submit("1").resultFuture().get(3, TimeUnit.SECONDS);
            assertTrue(result.isSuccess(), String.valueOf(result.getError()));
            assertNull(finalAuth.get());
            assertNull(finalCustom.get());
        } finally {
            fe.stop(0);
            firstRedirect.stop(0);
            finalServer.stop(0);
        }
    }

    @Test
    void configSeparatesUploadRequestUploadAndStatusPollTimeouts() {
        StreamLoadConfig cfg = StreamLoadConfig.builder()
                .endpoint("http://localhost:8030")
                .database("db")
                .table("t")
                .columns("id")
                .uploadRequestTimeout(Duration.ofSeconds(11))
                .uploadTimeout(Duration.ofSeconds(12))
                .statusPollTimeout(Duration.ofSeconds(13))
                .build();

        assertEquals(Duration.ofSeconds(11), cfg.getUploadRequestTimeout());
        assertEquals(Duration.ofSeconds(12), cfg.getUploadTimeout());
        assertEquals(Duration.ofSeconds(13), cfg.getStatusPollTimeout());
    }

    @Test
    void configDefaultsMatchGoParityDefaults() {
        StreamLoadConfig cfg = StreamLoadConfig.builder()
                .endpoint("http://localhost:8030")
                .database("db")
                .table("t")
                .columns("id")
                .build();

        assertEquals(Mode.CSV, cfg.getMode());
        assertEquals(",", cfg.getCsvSeparator());
        assertEquals("\"", cfg.getCsvEnclose());
        assertEquals(100_000, cfg.getMaxQueueSize());
        assertEquals(1, cfg.getMaxUploadQueueSize());
        assertEquals(90L * 1024 * 1024, cfg.getBatchBytes());
        assertEquals(Duration.ofMillis(5), cfg.getLinger());
        assertNull(cfg.getMaxQueueWaitTime());
        assertEquals(1, cfg.getUploadWorkers());
        assertEquals(Duration.ofSeconds(300), cfg.getUploadRequestTimeout());
        assertEquals(Duration.ofSeconds(300), cfg.getUploadTimeout());
        assertEquals(Duration.ofSeconds(300), cfg.getStatusPollTimeout());
        assertEquals(ValidationMode.SYNTAX, cfg.getValidationMode());
        assertFalse(cfg.isFakeSend());
        assertEquals(Duration.ofMillis(500), cfg.getFakeSendDelay());
        assertFalse(cfg.isTlsSkipVerify());
        assertEquals("java_stream_load", cfg.getLabelPrefix());
        assertEquals(Duration.ofMillis(10), cfg.getSlowCallbackWarn());
        assertNull(cfg.getUsername());
    }

    @Test
    void configBuilderAliasesAndCollectionsAreImmutable() {
        HttpClient httpClient = HttpClient.newHttpClient();
        StreamLoadConfig cfg = StreamLoadConfig.builder()
                .streamLoadUrl("https://fe-host:8030/api/db/t/_stream_load")
                .basicAuth("root", "secret")
                .mode(Mode.JSON)
                .columns("id", "name")
                .csvQuote("'")
                .maxQueueSize(7)
                .maxUploadQueueSize(3)
                .batchBytes(1024)
                .linger(Duration.ofMillis(9))
                .maxQueueWaitTime(Duration.ofMillis(8))
                .uploadWorkers(2)
                .uploadRequestTimeout(Duration.ofSeconds(11))
                .uploadTimeout(Duration.ofSeconds(12))
                .statusPollTimeout(Duration.ofSeconds(13))
                .validationMode(ValidationMode.STRICT)
                .fakeSend(true)
                .fakeSendDelay(Duration.ofMillis(4))
                .tlsSkipVerify(true)
                .tlsCaCertPath("/tmp/ca.pem")
                .labelPrefix("test_prefix")
                .httpClient(httpClient)
                .slowCallbackWarn(Duration.ofMillis(6))
                .customHeader("X-Test", "a")
                .customHeader("X-Test", "b")
                .build();

        assertEquals("https://fe-host:8030/api/db/t/_stream_load", cfg.getStreamLoadUrl());
        assertEquals("root", cfg.getUsername());
        assertEquals("secret", cfg.getPassword());
        assertEquals(Mode.JSON, cfg.getMode());
        assertEquals(List.of("id", "name"), cfg.getColumns());
        assertEquals("'", cfg.getCsvEnclose());
        assertEquals(7, cfg.getMaxQueueSize());
        assertEquals(3, cfg.getMaxUploadQueueSize());
        assertEquals(1024, cfg.getBatchBytes());
        assertEquals(Duration.ofMillis(9), cfg.getLinger());
        assertEquals(Duration.ofMillis(8), cfg.getMaxQueueWaitTime());
        assertEquals(2, cfg.getUploadWorkers());
        assertEquals(ValidationMode.STRICT, cfg.getValidationMode());
        assertTrue(cfg.isFakeSend());
        assertTrue(cfg.isTlsSkipVerify());
        assertEquals("/tmp/ca.pem", cfg.getTlsCaCertPath());
        assertEquals("test_prefix", cfg.getLabelPrefix());
        assertSame(httpClient, cfg.getHttpClient());
        assertEquals(Duration.ofMillis(6), cfg.getSlowCallbackWarn());
        assertEquals(List.of("a", "b"), cfg.getCustomHeaders().get("X-Test"));
        assertThrows(UnsupportedOperationException.class, () -> cfg.getColumns().add("age"));
        assertThrows(UnsupportedOperationException.class, () -> cfg.getCustomHeaders().put("x", List.of("y")));
    }

    @Test
    void configValidationRejectsInvalidValues() {
        assertThrows(IllegalArgumentException.class, () -> StreamLoadConfig.builder()
                .streamLoadUrl("ftp://localhost/api/db/t/_stream_load").columns("id").build());
        assertThrows(IllegalArgumentException.class, () -> StreamLoadConfig.builder()
                .endpoint("http://localhost:8030/path").database("db").table("t").columns("id").build());
        assertThrows(IllegalArgumentException.class, () -> StreamLoadConfig.builder()
                .endpoint("http://user@localhost:8030").database("db").table("t").columns("id").build());
        assertThrows(IllegalArgumentException.class, () -> StreamLoadConfig.builder()
                .endpoint("http://localhost:8030?x=1").database("db").table("t").columns("id").build());
        assertThrows(IllegalArgumentException.class, () -> StreamLoadConfig.builder()
                .endpoint("http://localhost:8030#frag").database("db").table("t").columns("id").build());
        assertThrows(IllegalArgumentException.class, () -> StreamLoadConfig.builder()
                .streamLoadUrl("http://localhost:8030/api/db/t/_stream_load#frag").columns("id").build());
        assertThrows(IllegalArgumentException.class, () -> StreamLoadConfig.builder()
                .endpoint("http://localhost:8030").database("db").table("t").build());
        assertThrows(IllegalArgumentException.class, () -> StreamLoadConfig.builder()
                .endpoint("not-a-url").database("db").table("t").columns("id").build());
        assertThrows(IllegalArgumentException.class, () -> StreamLoadConfig.builder()
                .endpoint("http://localhost:8030").database("db").table("t").columns("id").uploadWorkers(0).build());
        assertThrows(IllegalArgumentException.class, () -> StreamLoadConfig.builder()
                .endpoint("http://localhost:8030").database("db").table("t").columns("id").maxUploadQueueSize(0).build());
        assertThrows(IllegalArgumentException.class, () -> StreamLoadConfig.builder()
                .endpoint("http://localhost:8030").database("db").table("t").columns("id").batchBytes(0).build());
        assertThrows(IllegalArgumentException.class, () -> StreamLoadConfig.builder()
                .endpoint("http://localhost:8030").database("db").table("t").columns("id").batchBytes(91L * 1024 * 1024).build());
        assertThrows(IllegalArgumentException.class, () -> StreamLoadConfig.builder()
                .endpoint("http://localhost:8030").database("db").table("t").columns("id").maxQueueSize(0).build());
        assertThrows(IllegalArgumentException.class, () -> StreamLoadConfig.builder()
                .endpoint("http://localhost:8030").database("db").table("t").columns("id").maxQueueWaitTime(Duration.ofMillis(-1)).build());
        assertThrows(IllegalArgumentException.class, () -> StreamLoadConfig.builder()
                .endpoint("http://localhost:8030").database("db").table("t").columns("id").uploadRequestTimeout(Duration.ZERO).build());
        assertThrows(IllegalArgumentException.class, () -> StreamLoadConfig.builder()
                .endpoint("http://localhost:8030").database("db").table("t").columns("id").uploadRequestTimeout(Duration.ofSeconds(9)).build());
        assertThrows(IllegalArgumentException.class, () -> StreamLoadConfig.builder()
                .endpoint("http://localhost:8030").database("db").table("t").columns("id").uploadTimeout(Duration.ZERO).build());
        assertThrows(IllegalArgumentException.class, () -> StreamLoadConfig.builder()
                .endpoint("http://localhost:8030").database("db").table("t").columns("id").statusPollTimeout(Duration.ZERO).build());
        assertThrows(IllegalArgumentException.class, () -> StreamLoadConfig.builder()
                .endpoint("http://localhost:8030").database("db").table("t").columns("id").fakeSendDelay(Duration.ofMillis(-1)).build());
        assertThrows(IllegalArgumentException.class, () -> StreamLoadConfig.builder()
                .endpoint("http://localhost:8030").database("db").table("t").columns("id").labelPrefix(" ").build());
        assertThrows(IllegalArgumentException.class, () -> StreamLoadConfig.builder()
                .endpoint("http://localhost:8030").database("db").table("t").columns("id").slowCallbackWarn(Duration.ofMillis(-1)).build());
        assertThrows(IllegalArgumentException.class, () -> StreamLoadConfig.builder()
                .endpoint("http://localhost:8030").database("db").table("t").columns("id").mode(null).build());
        assertThrows(IllegalArgumentException.class, () -> StreamLoadConfig.builder()
                .endpoint("http://localhost:8030").database("db").table("t").columns("id").validationMode(null).build());
    }

    @Test
    void loaderConfigMapsAllFieldsAndRoundTripsJson() throws Exception {
        LoaderConfig loader = new LoaderConfig();
        loader.streamLoadUrl = "http://localhost:8030/api/db/t/_stream_load";
        loader.authenticationType = "basic";
        loader.authenticationToken = "root:secret";
        loader.mode = Mode.JSON;
        loader.columns = List.of("id", "name");
        loader.csvSeparator = "|";
        loader.csvQuote = "'";
        loader.maxQueueSize = 10;
        loader.maxUploadQueueSize = 2;
        loader.batchBytes = 4096L;
        loader.linger = "7ms";
        loader.maxQueueWaitTime = "8ms";
        loader.uploadWorkers = 3;
        loader.uploadRequestTimeout = "11s";
        loader.uploadTimeout = "12s";
        loader.statusPollTimeout = "13s";
        loader.validationMode = ValidationMode.STRICT;
        loader.fakeSend = true;
        loader.fakeSendDelay = "4ms";
        loader.tlsSkipVerify = true;
        loader.tlsCaCertPath = "/tmp/ca.pem";
        loader.labelPrefix = "loader_prefix";
        loader.customHeaders = Map.of("X-One", List.of("a", "b"));
        loader.slowCallbackWarn = "6ms";

        Path path = Files.createTempFile("loader-config", ".json");
        try {
            loader.save(path);
            String json = Files.readString(path);
            assertTrue(json.contains("\"stream_load_url\""));
            assertTrue(json.contains("\"authentication_type\""));
            assertTrue(json.contains("\"doris_upload_request_timeout\""));
            assertFalse(json.contains("streamLoadUrl"));
            StreamLoadConfig cfg = LoaderConfig.load(path).toConfig();
            assertEquals("http://localhost:8030/api/db/t/_stream_load", cfg.getStreamLoadUrl());
            assertEquals("root", cfg.getUsername());
            assertEquals("secret", cfg.getPassword());
            assertEquals(Mode.JSON, cfg.getMode());
            assertEquals(List.of("id", "name"), cfg.getColumns());
            assertEquals("|", cfg.getCsvSeparator());
            assertEquals("'", cfg.getCsvEnclose());
            assertEquals(10, cfg.getMaxQueueSize());
            assertEquals(2, cfg.getMaxUploadQueueSize());
            assertEquals(4096, cfg.getBatchBytes());
            assertEquals(Duration.ofMillis(7), cfg.getLinger());
            assertEquals(Duration.ofMillis(8), cfg.getMaxQueueWaitTime());
            assertEquals(3, cfg.getUploadWorkers());
            assertEquals(Duration.ofSeconds(11), cfg.getUploadRequestTimeout());
            assertEquals(Duration.ofSeconds(12), cfg.getUploadTimeout());
            assertEquals(Duration.ofSeconds(13), cfg.getStatusPollTimeout());
            assertEquals(ValidationMode.STRICT, cfg.getValidationMode());
            assertTrue(cfg.isFakeSend());
            assertEquals(Duration.ofMillis(4), cfg.getFakeSendDelay());
            assertTrue(cfg.isTlsSkipVerify());
            assertEquals("/tmp/ca.pem", cfg.getTlsCaCertPath());
            assertEquals("loader_prefix", cfg.getLabelPrefix());
            assertEquals(List.of("a", "b"), cfg.getCustomHeaders().get("X-One"));
            assertEquals(Duration.ofMillis(6), cfg.getSlowCallbackWarn());
        } finally {
            Files.deleteIfExists(path);
        }
    }

    @Test
    void loaderConfigParsesGoStyleDurationUnits() {
        assertEquals(Duration.ofMillis(500), LoaderConfig.parseDuration("500ms"));
        assertEquals(Duration.ofSeconds(2), LoaderConfig.parseDuration("2s"));
        assertEquals(Duration.ofMinutes(3), LoaderConfig.parseDuration("3m"));
        assertEquals(Duration.ofHours(4), LoaderConfig.parseDuration("4h"));
        assertThrows(Exception.class, () -> LoaderConfig.parseDuration("PT0.25S"));
    }

    @Test
    void realCsvUploadSendsExpectedHeadersAndBody() throws Exception {
        AtomicReference<String> bodyRef = new AtomicReference<>();
        AtomicReference<String> columnsRef = new AtomicReference<>();
        AtomicReference<String> separatorRef = new AtomicReference<>();
        AtomicReference<String> encloseRef = new AtomicReference<>();
        AtomicReference<String> formatRef = new AtomicReference<>();
        AtomicReference<String> contentTypeRef = new AtomicReference<>();

        HttpServer server = HttpServer.create(new java.net.InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/api/db/t/_stream_load", exchange -> {
            bodyRef.set(new String(exchange.getRequestBody().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8));
            columnsRef.set(exchange.getRequestHeaders().getFirst("columns"));
            separatorRef.set(exchange.getRequestHeaders().getFirst("column_separator"));
            encloseRef.set(exchange.getRequestHeaders().getFirst("enclose"));
            formatRef.set(exchange.getRequestHeaders().getFirst("format"));
            contentTypeRef.set(exchange.getRequestHeaders().getFirst("Content-Type"));
            byte[] body = "{\"Status\":\"Success\",\"NumberTotalRows\":2,\"NumberLoadedRows\":2}"
                    .getBytes(java.nio.charset.StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();

        StreamLoadConfig cfg = StreamLoadConfig.builder()
                .endpoint("http://127.0.0.1:" + server.getAddress().getPort())
                .database("db")
                .table("t")
                .mode(Mode.CSV)
                .columns("id", "name")
                .csvSeparator("|")
                .csvQuote("'")
                .validationMode(ValidationMode.NONE)
                .linger(Duration.ofMillis(1))
                .build();

        try (StreamLoadClient client = new StreamLoadClient(cfg)) {
            DeliveryResult result = client.submitBatch(List.of("1|'Alice'", "2|'Bob'"))
                    .resultFuture().get(3, TimeUnit.SECONDS);
            assertTrue(result.isSuccess(), String.valueOf(result.getError()));
            assertEquals("1|'Alice'\n2|'Bob'", bodyRef.get());
            assertEquals("id,name", columnsRef.get());
            assertEquals("|", separatorRef.get());
            assertEquals("'", encloseRef.get());
            assertEquals("csv", formatRef.get());
            assertEquals("text/csv", contentTypeRef.get());
        } finally {
            server.stop(0);
        }
    }

    @Test
    void realJsonUploadSendsExpectedHeadersAndArrayBody() throws Exception {
        AtomicReference<String> bodyRef = new AtomicReference<>();
        AtomicReference<String> formatRef = new AtomicReference<>();
        AtomicReference<String> stripOuterArrayRef = new AtomicReference<>();
        AtomicReference<String> readJsonByLineRef = new AtomicReference<>();
        AtomicReference<String> contentTypeRef = new AtomicReference<>();

        HttpServer server = HttpServer.create(new java.net.InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/api/db/t/_stream_load", exchange -> {
            bodyRef.set(new String(exchange.getRequestBody().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8));
            formatRef.set(exchange.getRequestHeaders().getFirst("format"));
            stripOuterArrayRef.set(exchange.getRequestHeaders().getFirst("strip_outer_array"));
            readJsonByLineRef.set(exchange.getRequestHeaders().getFirst("read_json_by_line"));
            contentTypeRef.set(exchange.getRequestHeaders().getFirst("Content-Type"));
            byte[] body = "{\"Status\":\"Success\",\"NumberTotalRows\":2,\"NumberLoadedRows\":2}"
                    .getBytes(java.nio.charset.StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();

        StreamLoadConfig cfg = StreamLoadConfig.builder()
                .endpoint("http://127.0.0.1:" + server.getAddress().getPort())
                .database("db")
                .table("t")
                .mode(Mode.JSON)
                .columns("id")
                .linger(Duration.ofMillis(1))
                .build();

        try (StreamLoadClient client = new StreamLoadClient(cfg)) {
            DeliveryResult result = client.submitBatch(List.of("{\"id\":1}", "{\"id\":2}"))
                    .resultFuture().get(3, TimeUnit.SECONDS);
            assertTrue(result.isSuccess(), String.valueOf(result.getError()));
            assertEquals("[{\"id\":1},{\"id\":2}]", bodyRef.get());
            assertEquals("json", formatRef.get());
            assertEquals("true", stripOuterArrayRef.get());
            assertEquals("false", readJsonByLineRef.get());
            assertEquals("application/json", contentTypeRef.get());
        } finally {
            server.stop(0);
        }
    }

    @Test
    void streamLoadUrlOnlyConfigPollsDatabaseFromUrlAfterAmbiguousResponse() throws Exception {
        AtomicReference<String> pollPath = new AtomicReference<>();
        AtomicReference<String> pollQuery = new AtomicReference<>();
        HttpServer server = HttpServer.create(new java.net.InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/api/url_db/url_table/_stream_load", exchange -> {
            exchange.sendResponseHeaders(200, -1);
            exchange.close();
        });
        server.createContext("/api/url_db/get_load_state", exchange -> {
            pollPath.set(exchange.getRequestURI().getPath());
            pollQuery.set(exchange.getRequestURI().getQuery());
            byte[] body = "{\"data\":\"VISIBLE\",\"msg\":\"ok\"}".getBytes(java.nio.charset.StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();

        StreamLoadConfig cfg = StreamLoadConfig.builder()
                .streamLoadUrl("http://127.0.0.1:" + server.getAddress().getPort()
                        + "/api/url_db/url_table/_stream_load")
                .mode(Mode.CSV)
                .columns("id")
                .uploadRequestTimeout(Duration.ofSeconds(10))
                .statusPollTimeout(Duration.ofSeconds(2))
                .linger(Duration.ofMillis(1))
                .build();

        try (StreamLoadClient client = new StreamLoadClient(cfg)) {
            DeliveryResult result = client.submit("1").resultFuture().get(3, TimeUnit.SECONDS);
            assertTrue(result.isSuccess(), String.valueOf(result.getError()));
            assertEquals("/api/url_db/get_load_state", pollPath.get());
            assertNotNull(pollQuery.get());
            assertTrue(pollQuery.get().contains("label="));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void responseStatusPublishTimeoutAndFinishedDuplicateLabelsAreSuccess() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        HttpServer server = HttpServer.create(new java.net.InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/api/db/t/_stream_load", exchange -> {
            int attempt = attempts.incrementAndGet();
            String json = attempt == 1
                    ? "{\"Status\":\"Publish Timeout\",\"Message\":\"publish pending\"}"
                    : "{\"Status\":\"Label Already Exists\",\"ExistingJobStatus\":\"FINISHED\",\"Message\":\"done\"}";
            byte[] body = json.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();

        StreamLoadConfig cfg = StreamLoadConfig.builder()
                .endpoint("http://127.0.0.1:" + server.getAddress().getPort())
                .database("db")
                .table("t")
                .mode(Mode.CSV)
                .columns("id")
                .linger(Duration.ofMillis(1))
                .build();

        try (StreamLoadClient client = new StreamLoadClient(cfg)) {
            assertTrue(client.submit("1").resultFuture().get(3, TimeUnit.SECONDS).isSuccess());
            assertTrue(client.submit("2").resultFuture().get(3, TimeUnit.SECONDS).isSuccess());
            assertEquals(2, attempts.get());
        } finally {
            server.stop(0);
        }
    }

    @Test
    void retriableHttpStatusUsesTotalUploadTimeoutBudget() throws Exception {
        AtomicInteger uploadAttempts = new AtomicInteger();
        HttpServer server = HttpServer.create(new java.net.InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/api/db/t/_stream_load", exchange -> {
            uploadAttempts.incrementAndGet();
            byte[] body = "{\"Status\":\"Fail\",\"Message\":\"busy\"}".getBytes(java.nio.charset.StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(503, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();

        StreamLoadConfig cfg = StreamLoadConfig.builder()
                .endpoint("http://127.0.0.1:" + server.getAddress().getPort())
                .database("db")
                .table("t")
                .mode(Mode.CSV)
                .columns("id")
                .uploadRequestTimeout(Duration.ofSeconds(10))
                .uploadTimeout(Duration.ofMillis(250))
                .linger(Duration.ofMillis(1))
                .build();

        try (StreamLoadClient client = new StreamLoadClient(cfg)) {
            DeliveryResult result = client.submit("1").resultFuture().get(3, TimeUnit.SECONDS);
            assertFalse(result.isSuccess());
            assertTrue(result.getError().getMessage().contains("uploadTimeout"),
                    result.getError().toString());
            assertEquals(1, uploadAttempts.get());
        } finally {
            server.stop(0);
        }
    }

    @Test
    void ambiguousUploadPollsUntilStatusPollTimeout() throws Exception {
        AtomicInteger pollAttempts = new AtomicInteger();
        HttpServer server = HttpServer.create(new java.net.InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/api/db/t/_stream_load", exchange -> {
            exchange.sendResponseHeaders(200, -1);
            exchange.close();
        });
        server.createContext("/api/db/get_load_state", exchange -> {
            pollAttempts.incrementAndGet();
            byte[] body = "{\"data\":\"PREPARE\",\"msg\":\"ok\"}".getBytes(java.nio.charset.StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();

        StreamLoadConfig cfg = StreamLoadConfig.builder()
                .endpoint("http://127.0.0.1:" + server.getAddress().getPort())
                .database("db")
                .table("t")
                .mode(Mode.CSV)
                .columns("id")
                .uploadRequestTimeout(Duration.ofSeconds(10))
                .uploadTimeout(Duration.ofSeconds(2))
                .statusPollTimeout(Duration.ofMillis(250))
                .linger(Duration.ofMillis(1))
                .build();

        try (StreamLoadClient client = new StreamLoadClient(cfg)) {
            DeliveryResult result = client.submit("1").resultFuture().get(3, TimeUnit.SECONDS);
            assertFalse(result.isSuccess());
            assertTrue(result.getError().getMessage().contains("statusPollTimeout"),
                    result.getError().toString());
            assertTrue(pollAttempts.get() >= 1);
        } finally {
            server.stop(0);
        }
    }

    @Test
    void deliveryResultDuration() {
        DeliveryResult r = DeliveryResult.success(1, 200, null,
                java.time.Instant.ofEpochMilli(1000),
                java.time.Instant.ofEpochMilli(1250));
        assertEquals(250, r.getDurationMs());
    }

    @Test
    void allHandlesResolveUnderConcurrentSubmit() throws Exception {
        StreamLoadConfig cfg = StreamLoadConfig.builder()
                .endpoint("http://localhost:8030")
                .database("testdb")
                .table("testtable")
                .mode(Mode.CSV)
                .columns("id")
                .fakeSend(true)
                .fakeSendDelay(Duration.ofMillis(5))
                .linger(Duration.ofMillis(10))
                .build();

        int threads = 8;
        int recordsPerThread = 500;
        CopyOnWriteArrayList<StreamLoadHandle> handles = new CopyOnWriteArrayList<>();
        ExecutorService pool = Executors.newFixedThreadPool(threads);

        try (StreamLoadClient client = new StreamLoadClient(cfg)) {
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                futures.add(CompletableFuture.runAsync(() -> {
                    for (int i = 0; i < recordsPerThread; i++) {
                        try {
                            handles.add(client.submit(Integer.toString(i)));
                        } catch (StreamLoadException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }, pool));
            }
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(30, TimeUnit.SECONDS);
        }

        pool.shutdown();
        assertEquals(threads * recordsPerThread, handles.size(), "every submission must produce a handle");
        for (StreamLoadHandle h : handles) {
            assertTrue(h.isDone(), "every handle must be resolved after close");
            assertNotNull(h.getResultNow(), "result must be non-null");
        }
    }

    @Test
    void allAcceptedHandlesResolveWhenCloseRacesSubmit() throws Exception {
        StreamLoadConfig cfg = StreamLoadConfig.builder()
                .endpoint("http://localhost:8030")
                .database("testdb")
                .table("testtable")
                .mode(Mode.CSV)
                .columns("id")
                .fakeSend(true)
                .fakeSendDelay(Duration.ofMillis(5))
                .linger(Duration.ofMillis(10))
                .maxQueueSize(100)
                .build();

        int threads = 4;
        int recordsPerThread = 200;
        CopyOnWriteArrayList<StreamLoadHandle> accepted = new CopyOnWriteArrayList<>();
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        StreamLoadClient client = new StreamLoadClient(cfg);

        List<CompletableFuture<Void>> submitters = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            submitters.add(CompletableFuture.runAsync(() -> {
                for (int i = 0; i < recordsPerThread; i++) {
                    try {
                        accepted.add(client.submit(Integer.toString(i)));
                    } catch (StreamLoadException e) {
                        // CLIENT_CLOSED or QUEUE_FULL: submission was rejected before enqueue,
                        // so no handle was produced — nothing to track.
                    }
                }
            }, pool));
        }

        // Close concurrently while submitters are still running.
        client.close();
        CompletableFuture.allOf(submitters.toArray(new CompletableFuture[0])).get(30, TimeUnit.SECONDS);
        pool.shutdown();

        // Every handle that was accepted must eventually resolve — no dangling futures.
        for (StreamLoadHandle h : accepted) {
            assertTrue(h.isDone(), "accepted handle must be resolved after close");
            assertNotNull(h.getResultNow());
        }
    }

    @Test
    void fakeSendZeroSuccessRateAllFuturesCompleteWithFailure() throws Exception {
        StreamLoadConfig cfg = StreamLoadConfig.builder()
                .endpoint("http://localhost:8030")
                .database("testdb")
                .table("testtable")
                .mode(Mode.CSV)
                .columns("id")
                .fakeSend(true)
                .fakeSendDelay(Duration.ofMillis(5))
                .fakeSendSuccessRate(0.0)
                .linger(Duration.ofMillis(10))
                .build();

        int total = 50;
        List<StreamLoadHandle> handles = new ArrayList<>(total);
        try (StreamLoadClient client = new StreamLoadClient(cfg)) {
            for (int i = 0; i < total; i++) {
                handles.add(client.submit(Integer.toString(i)));
            }
            for (StreamLoadHandle h : handles) {
                // Must complete within the timeout — no future should hang.
                DeliveryResult result = h.waitForResult(Duration.ofSeconds(5));
                assertFalse(result.isSuccess(), "result must be failure with 0% success rate");
                assertNotNull(result.getError(), "error must be set on failure");
                assertTrue(result.getError().getMessage().contains("fakeSendSuccessRate"),
                        "error message must identify the cause: " + result.getError().getMessage());
            }
        }

        assertEquals(total, handles.size());
    }

    @Test
    void callbackCountMatchesSubmissionCount() throws Exception {
        StreamLoadConfig cfg = StreamLoadConfig.builder()
                .endpoint("http://localhost:8030")
                .database("testdb")
                .table("testtable")
                .mode(Mode.CSV)
                .columns("id")
                .fakeSend(true)
                .fakeSendDelay(Duration.ofMillis(5))
                .linger(Duration.ofMillis(10))
                .build();

        int total = 500;
        AtomicInteger callbackCount = new AtomicInteger();
        List<StreamLoadHandle> handles = new ArrayList<>(total);

        try (StreamLoadClient client = new StreamLoadClient(cfg)) {
            for (int i = 0; i < total; i++) {
                handles.add(client.submitWithCallback(Integer.toString(i),
                        result -> callbackCount.incrementAndGet()));
            }
            for (StreamLoadHandle h : handles) {
                h.waitForResult(Duration.ofSeconds(10));
            }
        }

        assertEquals(total, callbackCount.get(),
                "exactly one callback must fire per submission");
    }
}
