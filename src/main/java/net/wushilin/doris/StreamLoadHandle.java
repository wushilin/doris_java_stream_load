package net.wushilin.doris;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A handle to an accepted stream-load submission.
 *
 * <p>The handle represents the first lifecycle boundary: the SDK accepted the
 * submission into its internal queue. The delivery outcome is represented by
 * {@link #resultFuture()} and completes later, after Doris accepts, rejects, or
 * times out the load.
 */
public class StreamLoadHandle {

    private final CompletableFuture<DeliveryResult> future;
    private final int recordCount;
    private final long byteSize;
    private final Instant submittedAt;

    StreamLoadHandle(CompletableFuture<DeliveryResult> future,
                     int recordCount,
                     long byteSize,
                     Instant submittedAt) {
        this.future = future;
        this.recordCount = recordCount;
        this.byteSize = byteSize;
        this.submittedAt = submittedAt;
    }

    /**
     * Block until the delivery completes (success or failure) and return the result.
     */
    public DeliveryResult waitForResult() throws InterruptedException, ExecutionException {
        return future.get();
    }

    /**
     * Block up to {@code timeout} for the delivery to complete.
     *
     * @throws TimeoutException if the timeout elapses before completion
     */
    public DeliveryResult waitForResult(Duration timeout)
            throws InterruptedException, ExecutionException, TimeoutException {
        return future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * Returns true if the delivery has completed (either success or failure).
     */
    public boolean isDone() {
        return future.isDone();
    }

    /**
     * Returns the delivery future for composing async chains.
     */
    public CompletableFuture<DeliveryResult> resultFuture() {
        return future;
    }

    /**
     * Alias for {@link #resultFuture()}.
     */
    public CompletableFuture<DeliveryResult> asFuture() {
        return resultFuture();
    }

    /** Number of logical records in this accepted submission. */
    public int getRecordCount() {
        return recordCount;
    }

    /** UTF-8 byte size of the outbound body if this submission were sent alone. */
    public long getByteSize() {
        return byteSize;
    }

    /** Time when the SDK accepted the submission into its queue. */
    public Instant getSubmittedAt() {
        return submittedAt;
    }

    /**
     * Returns the result immediately if already done, or throws {@link IllegalStateException}.
     */
    public DeliveryResult getResultNow() {
        if (!future.isDone()) {
            throw new IllegalStateException("Result is not yet available");
        }
        return future.getNow(null);
    }
}
