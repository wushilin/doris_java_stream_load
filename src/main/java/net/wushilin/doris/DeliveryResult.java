package net.wushilin.doris;

import java.time.Instant;
import java.util.Objects;

public class DeliveryResult {

    private final boolean success;
    private final Exception error;
    private final int attempts;
    private final int httpStatusCode;
    private final StreamLoadResponse response;
    private final Instant startTime;
    private final Instant finishTime;

    public DeliveryResult(boolean success, Exception error, int attempts,
                          int httpStatusCode, StreamLoadResponse response,
                          Instant startTime, Instant finishTime) {
        this.success = success;
        this.error = error;
        this.attempts = attempts;
        this.httpStatusCode = httpStatusCode;
        this.response = response;
        this.startTime = startTime;
        this.finishTime = finishTime;
    }

    public static DeliveryResult success(int attempts, int httpStatus,
                                          StreamLoadResponse response,
                                          Instant startTime, Instant finishTime) {
        return new DeliveryResult(true, null, attempts, httpStatus, response, startTime, finishTime);
    }

    public static DeliveryResult failure(Exception error, int attempts, int httpStatus,
                                          Instant startTime, Instant finishTime) {
        return new DeliveryResult(false, error, attempts, httpStatus, null, startTime, finishTime);
    }

    public boolean isSuccess() { return success; }
    public Exception getError() { return error; }
    public int getAttempts() { return attempts; }
    public int getHttpStatusCode() { return httpStatusCode; }
    public StreamLoadResponse getResponse() { return response; }
    public Instant getStartTime() { return startTime; }
    public Instant getFinishTime() { return finishTime; }

    public long getDurationMs() {
        if (startTime == null || finishTime == null) return 0;
        return finishTime.toEpochMilli() - startTime.toEpochMilli();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DeliveryResult)) return false;
        DeliveryResult that = (DeliveryResult) o;
        return success == that.success
                && attempts == that.attempts
                && httpStatusCode == that.httpStatusCode
                && Objects.equals(error, that.error)
                && Objects.equals(response, that.response)
                && Objects.equals(startTime, that.startTime)
                && Objects.equals(finishTime, that.finishTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(success, error, attempts, httpStatusCode, response, startTime, finishTime);
    }

    @Override
    public String toString() {
        return "DeliveryResult{success=" + success + ", attempts=" + attempts +
               ", httpStatus=" + httpStatusCode + ", durationMs=" + getDurationMs() + "}";
    }
}
