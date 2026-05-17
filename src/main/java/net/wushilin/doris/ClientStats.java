package net.wushilin.doris;

import java.util.Objects;

/**
 * A point-in-time snapshot of {@link StreamLoadClient} metrics.
 */
public class ClientStats {

    private final long totalRecordsSent;
    private final long totalRecordsFailed;
    private final long totalBatches;
    private final long totalBatchesFailed;
    private final long totalBytesSent;
    private final long totalRetries;
    private final long intakeQueueSize;
    private final long uploadQueueSize;

    public ClientStats(long totalRecordsSent, long totalRecordsFailed,
                       long totalBatches, long totalBatchesFailed,
                       long totalBytesSent, long totalRetries,
                       long intakeQueueSize, long uploadQueueSize) {
        this.totalRecordsSent = totalRecordsSent;
        this.totalRecordsFailed = totalRecordsFailed;
        this.totalBatches = totalBatches;
        this.totalBatchesFailed = totalBatchesFailed;
        this.totalBytesSent = totalBytesSent;
        this.totalRetries = totalRetries;
        this.intakeQueueSize = intakeQueueSize;
        this.uploadQueueSize = uploadQueueSize;
    }

    public long getTotalRecordsSent() { return totalRecordsSent; }
    public long getTotalRecordsFailed() { return totalRecordsFailed; }
    public long getTotalBatches() { return totalBatches; }
    public long getTotalBatchesFailed() { return totalBatchesFailed; }
    public long getTotalBytesSent() { return totalBytesSent; }
    public long getTotalRetries() { return totalRetries; }
    public long getIntakeQueueSize() { return intakeQueueSize; }
    public long getUploadQueueSize() { return uploadQueueSize; }

    /** Records that completed delivery (success + failure). */
    public long getTotalRecordsProcessed() {
        return totalRecordsSent + totalRecordsFailed;
    }

    /** Batches that completed delivery (success + failure). */
    public long getTotalBatchesProcessed() {
        return totalBatches + totalBatchesFailed;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ClientStats)) return false;
        ClientStats that = (ClientStats) o;
        return totalRecordsSent == that.totalRecordsSent
                && totalRecordsFailed == that.totalRecordsFailed
                && totalBatches == that.totalBatches
                && totalBatchesFailed == that.totalBatchesFailed
                && totalBytesSent == that.totalBytesSent
                && totalRetries == that.totalRetries
                && intakeQueueSize == that.intakeQueueSize
                && uploadQueueSize == that.uploadQueueSize;
    }

    @Override
    public int hashCode() {
        return Objects.hash(totalRecordsSent, totalRecordsFailed, totalBatches,
                totalBatchesFailed, totalBytesSent, totalRetries, intakeQueueSize, uploadQueueSize);
    }

    @Override
    public String toString() {
        return "ClientStats{" +
               "records=" + totalRecordsSent + "/" + getTotalRecordsProcessed() +
               ", batches=" + totalBatches + "/" + getTotalBatchesProcessed() +
               ", bytes=" + totalBytesSent +
               ", retries=" + totalRetries +
               ", intakeQueue=" + intakeQueueSize +
               ", uploadQueue=" + uploadQueueSize +
               '}';
    }
}
