package net.wushilin.doris;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class StreamLoadResponse {

    @JsonProperty("TxnId")
    private long txnId;

    @JsonProperty("Label")
    private String label;

    @JsonProperty("Status")
    private String status;

    @JsonProperty("ExistingJobStatus")
    private String existingJobStatus;

    @JsonProperty("Message")
    private String message;

    @JsonProperty("NumberTotalRows")
    private long totalRows;

    @JsonProperty("NumberLoadedRows")
    private long loadedRows;

    @JsonProperty("NumberFilteredRows")
    private long filteredRows;

    @JsonProperty("NumberUnselectedRows")
    private long unselectedRows;

    @JsonProperty("LoadBytes")
    private long loadBytes;

    @JsonProperty("LoadTimeMs")
    private long loadTimeMs;

    @JsonProperty("BeginTxnTimeMs")
    private long beginTxnTimeMs;

    @JsonProperty("StreamLoadPutTimeMs")
    private long streamLoadPutTimeMs;

    @JsonProperty("ReadDataTimeMs")
    private long readDataTimeMs;

    @JsonProperty("WriteDataTimeMs")
    private long writeDataTimeMs;

    @JsonProperty("CommitAndPublishTimeMs")
    private long commitAndPublishTimeMs;

    @JsonProperty("ErrorURL")
    private String errorUrl;

    static StreamLoadResponse fakeSuccess(String label, long rows, long bytes, long loadTimeMs) {
        StreamLoadResponse response = new StreamLoadResponse();
        response.label = label;
        response.status = "Success";
        response.message = "fake send success";
        response.totalRows = rows;
        response.loadedRows = rows;
        response.loadBytes = bytes;
        response.loadTimeMs = loadTimeMs;
        return response;
    }

    public boolean isSuccess() {
        // Null or empty Status matches Go's `case ""`: treat as success.
        if (status == null || status.isEmpty()) return true;
        if (status.equalsIgnoreCase("success") || status.equalsIgnoreCase("publish timeout")) {
            return true;
        }
        return status.equalsIgnoreCase("label already exists")
                && existingJobStatus != null
                && existingJobStatus.equalsIgnoreCase("finished");
    }

    public long getTxnId() { return txnId; }
    public String getLabel() { return label; }
    public String getStatus() { return status; }
    public String getExistingJobStatus() { return existingJobStatus; }
    public String getMessage() { return message; }
    public long getTotalRows() { return totalRows; }
    public long getLoadedRows() { return loadedRows; }
    public long getFilteredRows() { return filteredRows; }
    public long getUnselectedRows() { return unselectedRows; }
    public long getLoadBytes() { return loadBytes; }
    public long getLoadTimeMs() { return loadTimeMs; }
    public long getBeginTxnTimeMs() { return beginTxnTimeMs; }
    public long getStreamLoadPutTimeMs() { return streamLoadPutTimeMs; }
    public long getReadDataTimeMs() { return readDataTimeMs; }
    public long getWriteDataTimeMs() { return writeDataTimeMs; }
    public long getCommitAndPublishTimeMs() { return commitAndPublishTimeMs; }
    public String getErrorUrl() { return errorUrl; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof StreamLoadResponse)) return false;
        StreamLoadResponse that = (StreamLoadResponse) o;
        return txnId == that.txnId
                && totalRows == that.totalRows
                && loadedRows == that.loadedRows
                && filteredRows == that.filteredRows
                && unselectedRows == that.unselectedRows
                && loadBytes == that.loadBytes
                && loadTimeMs == that.loadTimeMs
                && beginTxnTimeMs == that.beginTxnTimeMs
                && streamLoadPutTimeMs == that.streamLoadPutTimeMs
                && readDataTimeMs == that.readDataTimeMs
                && writeDataTimeMs == that.writeDataTimeMs
                && commitAndPublishTimeMs == that.commitAndPublishTimeMs
                && Objects.equals(label, that.label)
                && Objects.equals(status, that.status)
                && Objects.equals(existingJobStatus, that.existingJobStatus)
                && Objects.equals(message, that.message)
                && Objects.equals(errorUrl, that.errorUrl);
    }

    @Override
    public int hashCode() {
        return Objects.hash(txnId, label, status, existingJobStatus, message, totalRows,
                loadedRows, filteredRows, unselectedRows, loadBytes, loadTimeMs,
                beginTxnTimeMs, streamLoadPutTimeMs, readDataTimeMs, writeDataTimeMs,
                commitAndPublishTimeMs, errorUrl);
    }

    @Override
    public String toString() {
        return "StreamLoadResponse{status='" + status + "', label='" + label +
               "', loadedRows=" + loadedRows + ", filteredRows=" + filteredRows +
               ", message='" + message + "'}";
    }
}
