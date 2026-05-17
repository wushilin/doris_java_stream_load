# Doris Stream Load Java SDK

`doris-stream-load` is a Java SDK for sending CSV rows or JSON objects to Apache Doris Stream Load.

The target public model is intentionally small:

- `Mode.CSV` and `Mode.JSON`
- string input only
- `submit(...)` for one item
- `submitBatch(...)` for many items
- `submitAsync(...)` / `submitBatchAsync(...)` when queue admission itself should be asynchronous
- batching, queueing, retry, callback, handle, and stats are handled inside the SDK

This README is the Java target specification, aligned with
[`github.com/wushilin/doris_go_stream_load`](https://github.com/wushilin/doris_go_stream_load).

## Requirements

Java 17 or newer.

## Install

Gradle:

```gradle
dependencies {
    implementation "net.wushilin:doris-stream-load:1.0.0"
}
```

Maven:

```xml
<dependency>
  <groupId>net.wushilin</groupId>
  <artifactId>doris-stream-load</artifactId>
  <version>1.0.0</version>
</dependency>
```

## FakeSend Quick Start

This example is local runnable. It does not require a Doris cluster because `fakeSend(true)` bypasses real HTTP upload and returns a successful Stream Load result.

```java
package demo;

import net.wushilin.doris.DeliveryResult;
import net.wushilin.doris.Mode;
import net.wushilin.doris.StreamLoadClient;
import net.wushilin.doris.StreamLoadConfig;
import net.wushilin.doris.StreamLoadHandle;
import net.wushilin.doris.ValidationMode;

import java.time.Duration;
import java.util.List;

public class Main {
    public static void main(String[] args) throws Exception {
        StreamLoadConfig config = StreamLoadConfig.builder()
                .endpoint("http://example.invalid")
                .database("demo")
                .table("events")
                .columns("event_time", "user_id", "event_name")
                .mode(Mode.CSV)
                .validationMode(ValidationMode.SYNTAX)
                .batchBytes(1024 * 1024)
                .linger(Duration.ofMillis(10))
                .uploadWorkers(1)
                .uploadTimeout(Duration.ofSeconds(30))
                .fakeSend(true)
                .fakeSendDelay(Duration.ofMillis(20))
                .build();

        try (StreamLoadClient client = new StreamLoadClient(config)) {
            StreamLoadHandle handle = client.submitBatch(List.of(
                    "2026-05-01T10:00:00Z,1,login",
                    "2026-05-01T10:00:01Z,2,logout"
            ));

            DeliveryResult result = handle.resultFuture().get();
            if (!result.isSuccess()) {
                throw result.getError();
            }

            System.out.printf("success label=%s attempts=%d records=%d%n",
                    result.getResponse().getLabel(),
                    result.getAttempts(),
                    client.stats().getTotalRecordsSent());
        }
    }
}
```

## Real Doris Cluster

For a real cluster, create a table first. The following DDL assumes append-only event data, common time-range queries, and a small/general-purpose cluster. Tune partitions, buckets, and replication for your own data volume and cluster size.

```sql
CREATE DATABASE IF NOT EXISTS demo;

CREATE TABLE IF NOT EXISTS demo.events (
    event_time DATETIME NOT NULL,
    user_id BIGINT NOT NULL,
    event_name VARCHAR(64) NOT NULL
)
DUPLICATE KEY(event_time, user_id)
PARTITION BY RANGE(event_time) ()
DISTRIBUTED BY HASH(user_id) BUCKETS 8
PROPERTIES (
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-30",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "replication_num" = "1",
    "compression" = "zstd"
);
```

Configure the SDK with either a full Stream Load URL:

```java
StreamLoadConfig config = StreamLoadConfig.builder()
        .streamLoadUrl("http://127.0.0.1:8030/api/demo/events/_stream_load")
        .columns("event_time", "user_id", "event_name")
        .mode(Mode.CSV)
        .basicAuth("root", "password")
        .build();
```

Or configure endpoint, database, and table separately:

```java
StreamLoadConfig config = StreamLoadConfig.builder()
        .endpoint("http://127.0.0.1:8030")
        .database("demo")
        .table("events")
        .columns("event_time", "user_id", "event_name")
        .mode(Mode.JSON)
        .basicAuth("root", "password")
        .build();
```

If Doris does not require auth, leave authentication unset.

## CSV And JSON

Each submitted string is one logical item.

In `Mode.CSV`, one string is one CSV row:

```java
client.submit("2026-05-01T10:00:00Z,1,login");
client.submitBatch(List.of(
        "2026-05-01T10:00:01Z,2,logout",
        "2026-05-01T10:00:02Z,3,purchase"
));
```

When several CSV items are coalesced into one outbound Doris request, the body is newline joined:

```text
row1
row2
row3
```

In `Mode.JSON`, one string is one JSON object:

```java
client.submit("{\"event_time\":\"2026-05-01T10:00:00Z\",\"user_id\":1,\"event_name\":\"login\"}");
client.submitBatch(List.of(
        "{\"event_time\":\"2026-05-01T10:00:01Z\",\"user_id\":2,\"event_name\":\"logout\"}",
        "{\"event_time\":\"2026-05-01T10:00:02Z\",\"user_id\":3,\"event_name\":\"purchase\"}"
));
```

When several JSON items are coalesced into one outbound Doris request, the body is one JSON array:

```json
[{"id":1},{"id":2},{"id":3}]
```

Validation is controlled by `StreamLoadConfig.validationMode(...)`:

| Value | CSV behavior | JSON behavior |
|---|---|---|
| `ValidationMode.NONE` | No parsing before queue admission | No parsing before queue admission |
| `ValidationMode.SYNTAX` | Non-blank, parses as one row, field count matches `columns` | Non-blank, valid JSON object |
| `ValidationMode.STRICT` | Same as syntax today | Valid object, every configured column present, no extra keys |

CSV formatting defaults to separator `,` and quote `"`. Override with `csvSeparator(...)` and `csvQuote(...)`.

## Submission, Handle, Future, Stats

The Java API exposes two lifecycle stages.

The first stage is queue admission. `submit(...)` and `submitBatch(...)` validate the input, create one handle/future for the submission, then block until the SDK either accepts the submission into its internal intake queue or throws `StreamLoadException`.

The second stage is Doris delivery. Once a submission is accepted, the returned `StreamLoadHandle` owns a `CompletableFuture<DeliveryResult>` exposed by `resultFuture()`.

```java
StreamLoadHandle handle = client.submitBatch(List.of(
        "2026-05-01T10:00:00Z,1,login",
        "2026-05-01T10:00:01Z,2,logout"
));

DeliveryResult result = handle.resultFuture().get();
if (!result.isSuccess()) {
    throw result.getError();
}
```

If queue admission itself should be asynchronous, use `submitAsync(...)` or `submitBatchAsync(...)`. The returned future completes with a `StreamLoadHandle` after admission; the handle's result future completes after Doris delivery:

```java
client.submitBatchAsync(records)
        .thenCompose(handle -> handle.resultFuture())
        .thenAccept(result -> {
            if (!result.isSuccess()) {
                System.err.println(result.getError());
            }
        });
```

`StreamLoadHandle` includes accepted-submission metadata:

- `waitForResult()`
- `waitForResult(Duration timeout)`
- `resultFuture()`
- `asFuture()`
- `isDone()`
- `getResultNow()`
- `getRecordCount()`
- `getByteSize()`
- `getSubmittedAt()`

Callbacks run once per submitted submission, not once per row:

```java
StreamLoadHandle handle = client.submitBatchWithCallback(List.of(
        "2026-05-01T10:00:00Z,1,login",
        "2026-05-01T10:00:01Z,2,logout"
), result -> {
    if (!result.isSuccess()) {
        System.out.printf("delivery failed attempts=%d err=%s%n",
                result.getAttempts(), result.getError());
        return;
    }
    System.out.printf("delivered label=%s%n", result.getResponse().getLabel());
});
```

The callback is invoked once for the caller's submission. A `submitBatch(...)` of 1,000 rows gets one callback. If several caller submissions are coalesced into one Doris upload, each caller submission still gets its own callback and its own future completion with the shared delivery result.

`DeliveryResult` includes success/error, attempts, status code, response, start time, and finish time.

Use `client.stats()` for a lifetime snapshot:

```java
ClientStats stats = client.stats();
System.out.printf("jobs=%d errors=%d records=%d bytes=%d%n",
        stats.getTotalBatchesProcessed(),
        stats.getTotalBatchesFailed(),
        stats.getTotalRecordsSent(),
        stats.getTotalBytesSent());
```

Current stats include record counts, batch counts, failed counts, bytes, retries, and queue sizes.

## Pipeline Behavior

The client uses a staged pipeline:

1. Producers call `submit(...)`, `submitBatch(...)`, or async variants.
2. Accepted submissions enter the bounded intake queue.
3. One batcher thread drains intake submissions, builds Doris request payloads, and hands sealed `DeliveryBatch` objects to the bounded upload queue.
4. One or more upload workers consume upload batches and perform fake-send or real Stream Load HTTP requests.

The intake side is many producers to one consumer, so the batcher uses vectorized dequeue. It waits for the first available submission, then drains up to 1,024 currently available submissions into a local reusable list. This reduces producer/consumer lock handoff without allowing the local drain buffer to grow to the full intake queue capacity.

The upload side remains single-item dequeue: each worker takes one already-formed `DeliveryBatch` at a time. This keeps upload work distributed across workers instead of letting one worker reserve many batches.

Batching is controlled by `batchBytes` and `linger`. A batch is flushed when either:

- adding more data would exceed `batchBytes`,
- the open batch reaches `batchBytes`,
- the open batch reaches `linger`,
- or `close()` asks the pipeline to finish.

If a drained submission does not fit in the current batch, the current batch is flushed and that submission starts the next batch. If the drained local list runs out before the new batch is full, the partial batch stays open and can be combined with the head of the next drain until `linger` expires.

Accepted futures are always completed eventually, either with success or failure. A submission that is rejected before queue admission throws and is not represented by an accepted handle. If the batcher or worker hits an uncaught exception, accepted submissions held in the current partial batch, the local drained list, the intake queue, or the upload batch are completed with failure rather than left dangling.

## Parameters

Required and routing:

| Field | Description |
|---|---|
| `columns` | Doris target columns in record order |
| `mode` | `Mode.CSV` or `Mode.JSON`; optional, defaults to `Mode.CSV` |
| `streamLoadUrl` | Full URL like `http://host:8030/api/db/table/_stream_load` |
| `endpoint` + `database` + `table` | Alternative to `streamLoadUrl` |

Connection and auth:

| Field | Default | Description |
|---|---|---|
| `username` / `password` | no auth / empty password | Basic-auth credentials; no Authorization header is sent when username is unset |
| `basicAuth(user, password)` | none | Convenience builder method for username/password |
| `authenticationType` / `authenticationToken` | none | `LoaderConfig` JSON compatibility; `"basic"` with token `user:password` |
| `customHeaders` / `headers` | empty | Extra HTTP headers sent to Doris |
| `tlsSkipVerify` | `false` | Skip TLS certificate verification |
| `tlsCaCertPath` | empty | Custom CA certificate path |
| `httpClient` | SDK-created client | Optional custom Java `HttpClient` |

Batching and queueing:

| Field | Default | Description |
|---|---|---|
| `batchBytes` | `90 MiB` | Max outbound request body size; also the per-send admission limit |
| `linger` | `5ms` | Max age of an open outbound batch before dispatch |
| `maxQueueSize` | `100000` | Max accepted submissions in the intake queue |
| `maxQueueWaitTime` | unset | How long submission waits for queue space; unset waits indefinitely |
| `maxUploadQueueSize` | `1` | Queue depth between batcher and upload workers |
| `uploadWorkers` | `1` | Concurrent upload workers |

`maxQueueWaitTime` is a full deadline for admission. Time spent waiting to acquire the queue lock and time spent waiting for queue capacity share the same timeout budget. `Duration.ZERO` means try once without waiting. `null` means block until accepted or closed.

Retry and timing:

| Field | Default | Description |
|---|---|---|
| `uploadRequestTimeout` | `300s` | HTTP deadline for one upload or label-poll request; minimum `10s` |
| `uploadTimeout` | `300s` | Total retry decision budget after retriable upload outcomes |
| `statusPollTimeout` | `300s` | Max time spent polling a label after an ambiguous outcome |
| `slowCallbackWarn` | `10ms` | Slow callback warning threshold |
| `maxRetries` | `3` | Deprecated compatibility field; retries are governed by `uploadTimeout` |

Behavior:

| Field | Default | Description |
|---|---|---|
| `validationMode` | `ValidationMode.SYNTAX` | `NONE`, `SYNTAX`, or `STRICT` |
| `labelPrefix` | `java_stream_load` | Prefix for generated Doris labels |
| `fakeSend` | `false` | Bypass real HTTP upload and return fake success |
| `fakeSendDelay` | `500ms` | Artificial fake-send delay |
| `fakeSendSuccessRate` | `1.0` | Fraction of fake-send batches that succeed |

`batchBytes` and `linger` work together like Kafka `batch.size` and `linger.ms`: the SDK dispatches when the payload reaches `batchBytes` or the open batch reaches `linger`, whichever happens first.

## Performance Guidelines

Start with defaults unless you already know the bottleneck. The defaults favor safety and predictable backpressure:

- `batchBytes = 90 MiB`
- `linger = 5ms`
- `maxQueueSize = 100000`
- `maxUploadQueueSize = 1`
- `uploadWorkers = 1`

For higher throughput:

- Prefer `submitBatch(...)` when the caller naturally has multiple rows. One caller batch uses one intake queue slot and one future.
- Set `validationMode(ValidationMode.NONE)` only when upstream data is already trusted; validation can dominate fake-send benchmarks.
- Increase `uploadWorkers` when Doris and the network can sustain concurrent Stream Load requests. Keep `maxUploadQueueSize` small at first; it is a buffer of sealed upload batches, not raw rows.
- Increase `linger` when producers are bursty and you want fuller batches. Decrease `linger` for lower tail latency.
- Keep `batchBytes` below Doris and infrastructure limits. The SDK caps it at 90 MiB.
- Use `maxQueueWaitTime` to bound producer latency under backpressure. Leave it unset only when producers are allowed to block indefinitely.
- Do not raise `maxQueueSize` as the first fix for slow uploads. A larger queue mostly stores more pending futures and data references; it does not make Doris ingest faster.
- Keep callbacks small. Slow callbacks run after futures complete but still use SDK worker threads for invocation bookkeeping and logging.

The hot intake path is optimized for many producers and one batcher: the batcher drains up to 1,024 submissions per lock acquisition. This is enough to reduce lock contention by orders of magnitude without doubling memory at large queue sizes. The upload queue intentionally does not bulk-drain because multiple workers should share sealed batches fairly.

Run the opt-in fake-send benchmark locally with:

```bash
./gradlew test -Dstreamload.perf=true
```

Useful knobs:

```bash
./gradlew test -Dstreamload.perf=true \
  -Dstreamload.perf.messages=20000000 \
  -Dstreamload.perf.wait.seconds=600
```

The benchmark writes a report under `build/reports/perf/`.

## LoaderConfig

`LoaderConfig` is the JSON-serializable counterpart of `StreamLoadConfig`.

```java
LoaderConfig loaderConfig = LoaderConfig.load(Path.of("loader.json"));
StreamLoadConfig config = loaderConfig.toConfig();

try (StreamLoadClient client = new StreamLoadClient(config)) {
    // send records
}
```

Duration fields use strings such as `"500ms"`, `"2s"`, and `"5m"`.

Example JSON:

```json
{
  "endpoint": "http://127.0.0.1:8030",
  "database": "demo",
  "table": "events",
  "mode": "CSV",
  "columns": ["event_time", "user_id", "event_name"],
  "authentication_type": "basic",
  "authentication_token": "root:password",
  "batch_bytes": 94371840,
  "linger": "5ms",
  "max_queue_size": 100000,
  "max_upload_queue_size": 1,
  "doris_upload_workers": 1,
  "doris_upload_request_timeout": "300s",
  "doris_upload_timeout": "300s",
  "status_poll_timeout": "300s",
  "validation": "SYNTAX"
}
```

## Common Errors

`submit(...)` and `submitBatch(...)` can fail before data enters the SDK queue:

| Error | Meaning | Typical action |
|---|---|---|
| `CLIENT_CLOSED` | The client is closed | Stop sending or create a new client |
| `QUEUE_FULL` | The intake queue stayed full longer than `maxQueueWaitTime` | Increase workers, queue size, or timeout; reduce producer rate |
| `RECORD_TOO_LARGE` | One submitted item or batch is larger than `batchBytes` | Split the caller-side batch or raise `batchBytes` up to the 90 MiB limit |
| validation error | CSV/JSON failed configured validation | Fix the row/object or loosen `validationMode` |

Delivery can fail after queue admission; check `DeliveryResult.getError()` from the handle or callback:

| Failure | Meaning | Typical action |
|---|---|---|
| HTTP 4xx from Doris | Bad URL, auth, table, schema, label, or data format | Inspect status code and Doris response message |
| HTTP 5xx or retriable transport error | Doris/BE/network may be unavailable | The SDK retries within `uploadTimeout`; check cluster health |
| ambiguous transport error | Request may have reached Doris but response was lost | The SDK polls the load label to decide whether it became visible |
| label state `UNKNOWN` | Doris does not know the label | The transaction was not registered; final result is failure |
| status poll timeout | Doris did not reach a final visible/failed state in time | Increase `statusPollTimeout` or inspect Doris load jobs |
| callback too slow | Callback exceeded `slowCallbackWarn` and produced an info log | Keep callbacks small; hand work to another executor if needed |

## Shutdown

`close()` is cooperative and does not interrupt worker logic.

Shutdown behavior:

1. The client marks itself closed.
2. The intake queue is atomically closed. After this point no new item can enter the intake queue.
3. Producers already waiting on admission wake and receive `CLIENT_CLOSED` if they were not accepted.
4. Already accepted intake submissions continue through the batcher.
5. The batcher drains accepted intake submissions, flushes any partial batch, then closes the upload queue.
6. Upload workers keep consuming already queued upload batches. When the upload queue is closed and empty, workers exit.
7. `close()` waits indefinitely for the batcher, admission executor, and upload workers to finish.

The boundary is strict at the queue level: after a queue is closed, no later enqueue can succeed. A submission is either not enqueued and fails admission, or it was accepted and its delivery future eventually completes with success or failure. `close()` itself has no timeout.

## Go Parity Notes

The Java implementation is aligned with the Go variant for string-based CSV/JSON submission, batching, fake send, safe redirect handling, retry/poll behavior, handles, callbacks, loader config, and validation modes. Java stats intentionally keep a smaller snapshot surface: record counts, batch counts, failed counts, bytes, retries, and queue sizes.
