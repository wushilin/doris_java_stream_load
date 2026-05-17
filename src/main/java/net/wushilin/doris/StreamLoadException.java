package net.wushilin.doris;

public class StreamLoadException extends Exception {
    private static final long serialVersionUID = 1L;

    public enum ErrorCode {
        CLIENT_CLOSED,
        QUEUE_FULL,
        RECORD_TOO_LARGE,
        INVALID_CONFIG,
        INVALID_RECORD,
        HTTP_ERROR,
        TIMEOUT,
        INTERNAL
    }

    private final ErrorCode code;

    public StreamLoadException(ErrorCode code, String message) {
        super(message);
        this.code = code;
    }

    public StreamLoadException(ErrorCode code, String message, Throwable cause) {
        super(message, cause);
        this.code = code;
    }

    public ErrorCode getCode() {
        return code;
    }

    @Override
    public String toString() {
        return "StreamLoadException[" + code + "]: " + getMessage();
    }
}
