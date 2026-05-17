package net.wushilin.doris;

import java.time.Duration;
import java.net.URI;
import java.net.http.HttpClient;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Immutable configuration for {@link StreamLoadClient}. Build with {@link #builder()}.
 *
 * <pre>{@code
 * StreamLoadConfig cfg = StreamLoadConfig.builder()
 *     .endpoint("http://doris-fe:8030")
 *     .database("mydb")
 *     .table("mytable")
 *     .username("root")
 *     .password("")
 *     .mode(Mode.JSON)
 *     .columns("id", "name")
 *     .build();
 * }</pre>
 */
public class StreamLoadConfig {

    private final String endpoint;
    private final String database;
    private final String table;
    private final String streamLoadUrl;
    private final String username;
    private final String password;
    private final Mode mode;
    private final List<String> columns;
    private final String csvSeparator;
    private final String csvEnclose;
    private final int maxQueueSize;
    private final int maxUploadQueueSize;
    private final long batchBytes;
    private final Duration linger;
    private final Duration maxQueueWaitTime;
    private final int uploadWorkers;
    private final Duration uploadRequestTimeout;
    private final Duration uploadTimeout;
    private final Duration statusPollTimeout;
    private final int maxRetries;
    private final ValidationMode validationMode;
    private final boolean fakeSend;
    private final Duration fakeSendDelay;
    private final double fakeSendSuccessRate;
    private final boolean tlsSkipVerify;
    private final String tlsCaCertPath;
    private final String labelPrefix;
    private final Map<String, List<String>> customHeaders;
    private final HttpClient httpClient;
    private final Duration slowCallbackWarn;

    private StreamLoadConfig(Builder b) {
        this.endpoint = b.endpoint;
        this.database = b.database;
        this.table = b.table;
        this.streamLoadUrl = b.streamLoadUrl;
        this.username = b.username;
        this.password = b.password;
        this.mode = b.mode;
        this.columns = b.columns != null
                ? Collections.unmodifiableList(new ArrayList<>(b.columns))
                : Collections.emptyList();
        this.csvSeparator = b.csvSeparator;
        this.csvEnclose = b.csvEnclose;
        this.maxQueueSize = b.maxQueueSize;
        this.maxUploadQueueSize = b.maxUploadQueueSize;
        this.batchBytes = b.batchBytes;
        this.linger = b.linger;
        this.maxQueueWaitTime = b.maxQueueWaitTime;
        this.uploadWorkers = b.uploadWorkers;
        this.uploadRequestTimeout = b.uploadRequestTimeout;
        this.uploadTimeout = b.uploadTimeout;
        this.statusPollTimeout = b.statusPollTimeout;
        this.maxRetries = b.maxRetries;
        this.validationMode = b.validationMode;
        this.fakeSend = b.fakeSend;
        this.fakeSendDelay = b.fakeSendDelay;
        this.fakeSendSuccessRate = b.fakeSendSuccessRate;
        this.tlsSkipVerify = b.tlsSkipVerify;
        this.tlsCaCertPath = b.tlsCaCertPath;
        this.labelPrefix = b.labelPrefix;
        this.httpClient = b.httpClient;
        this.slowCallbackWarn = b.slowCallbackWarn;
        Map<String, List<String>> copy = new LinkedHashMap<>();
        for (Map.Entry<String, List<String>> e : b.customHeaders.entrySet()) {
            copy.put(e.getKey(), Collections.unmodifiableList(new ArrayList<>(e.getValue())));
        }
        this.customHeaders = Collections.unmodifiableMap(copy);
    }

    /** Constructs the Doris stream load URL from endpoint, database, and table. */
    public String getStreamLoadUrl() {
        if (streamLoadUrl != null && !streamLoadUrl.isBlank()) {
            return streamLoadUrl;
        }
        return endpoint + "/api/" + database + "/" + table + "/_stream_load";
    }

    public String getEndpoint() { return endpoint; }
    public String getDatabase() { return database; }
    public String getTable() { return table; }
    public String getConfiguredStreamLoadUrl() { return streamLoadUrl; }
    public String getUsername() { return username; }
    public String getPassword() { return password; }
    public Mode getMode() { return mode; }
    public List<String> getColumns() { return columns; }
    public String getCsvSeparator() { return csvSeparator; }
    public String getCsvEnclose() { return csvEnclose; }
    public int getMaxQueueSize() { return maxQueueSize; }
    public int getMaxUploadQueueSize() { return maxUploadQueueSize; }
    public long getBatchBytes() { return batchBytes; }
    public Duration getLinger() { return linger; }
    public Duration getMaxQueueWaitTime() { return maxQueueWaitTime; }
    public int getUploadWorkers() { return uploadWorkers; }
    public Duration getUploadRequestTimeout() { return uploadRequestTimeout; }
    public Duration getUploadTimeout() { return uploadTimeout; }
    public Duration getStatusPollTimeout() { return statusPollTimeout; }
    public int getMaxRetries() { return maxRetries; }
    public ValidationMode getValidationMode() { return validationMode; }
    public boolean isFakeSend() { return fakeSend; }
    public Duration getFakeSendDelay() { return fakeSendDelay; }
    public double getFakeSendSuccessRate() { return fakeSendSuccessRate; }
    public boolean isTlsSkipVerify() { return tlsSkipVerify; }
    public String getTlsCaCertPath() { return tlsCaCertPath; }
    public String getLabelPrefix() { return labelPrefix; }
    public Map<String, List<String>> getCustomHeaders() { return customHeaders; }
    public HttpClient getHttpClient() { return httpClient; }
    public Duration getSlowCallbackWarn() { return slowCallbackWarn; }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private String endpoint;
        private String database;
        private String table;
        private String streamLoadUrl;
        private String username = null;
        private String password = "";
        private Mode mode = Mode.CSV;
        private List<String> columns;
        private String csvSeparator = ",";          // matches Go default
        private String csvEnclose = "\"";           // matches Go default (enclose header)
        private int maxQueueSize = 100_000;
        private int maxUploadQueueSize = 1;
        private long batchBytes = 90L * 1024 * 1024;   // 90 MiB
        private Duration linger = Duration.ofMillis(5);
        private Duration maxQueueWaitTime = null;        // null = block indefinitely
        private int uploadWorkers = 1;
        private Duration uploadRequestTimeout = Duration.ofSeconds(300);
        private Duration uploadTimeout = Duration.ofSeconds(300);
        private Duration statusPollTimeout = Duration.ofSeconds(300);
        private int maxRetries = 3;                      // deprecated, not enforced
        private ValidationMode validationMode = ValidationMode.SYNTAX;
        private boolean fakeSend = false;
        private Duration fakeSendDelay = Duration.ofMillis(500);
        private double fakeSendSuccessRate = 1.0;
        private boolean tlsSkipVerify = false;
        private String tlsCaCertPath = null;
        private String labelPrefix = "java_stream_load";
        private final Map<String, List<String>> customHeaders = new LinkedHashMap<>();
        private HttpClient httpClient = null;
        private Duration slowCallbackWarn = Duration.ofMillis(10);

        /** Doris FE base URL, e.g. {@code http://fe-host:8030}. Required. */
        public Builder endpoint(String endpoint) { this.endpoint = endpoint; return this; }

        /** Full Doris Stream Load URL, e.g. {@code http://fe:8030/api/db/table/_stream_load}. */
        public Builder streamLoadUrl(String streamLoadUrl) { this.streamLoadUrl = streamLoadUrl; return this; }

        /** Target database name. Required. */
        public Builder database(String database) { this.database = database; return this; }

        /** Target table name. Required. */
        public Builder table(String table) { this.table = table; return this; }

        /** Doris username. Default: none, so no Authorization header is sent. */
        public Builder username(String username) { this.username = username; return this; }

        /** Doris password. Default: empty string. */
        public Builder password(String password) { this.password = password; return this; }

        /** Configure basic authentication from username and password. */
        public Builder basicAuth(String username, String password) {
            this.username = username;
            this.password = password;
            return this;
        }

        /** Data format. Default: {@link Mode#CSV}. */
        public Builder mode(Mode mode) { this.mode = mode; return this; }

        /** Column list sent as the {@code columns} header (for CSV column mapping). */
        public Builder columns(String... columns) {
            this.columns = Arrays.asList(columns);
            return this;
        }

        /** Column list sent as the {@code columns} header. */
        public Builder columns(List<String> columns) {
            this.columns = new ArrayList<>(columns);
            return this;
        }

        /** CSV field separator. Default: {@code ,} (comma). */
        public Builder csvSeparator(String sep) { this.csvSeparator = sep; return this; }

        /**
         * CSV enclosing/quote character sent as the Doris {@code enclose} header.
         * Default: {@code "} (double quote). Set to empty string to omit the header.
         */
        public Builder csvEnclose(String enclose) { this.csvEnclose = enclose; return this; }

        /** Alias for {@link #csvEnclose(String)}. */
        public Builder csvQuote(String quote) { return csvEnclose(quote); }

        /** Maximum records buffered in the intake queue. Default: 100,000. */
        public Builder maxQueueSize(int size) { this.maxQueueSize = size; return this; }

        /** Maximum sealed upload batches waiting for workers. Default: 1. */
        public Builder maxUploadQueueSize(int size) { this.maxUploadQueueSize = size; return this; }

        /** Maximum bytes per upload batch. Default: 90 MiB. */
        public Builder batchBytes(long bytes) { this.batchBytes = bytes; return this; }

        /**
         * How long to wait before flushing a partial batch. Default: 5 ms.
         * Lower values reduce latency; higher values improve throughput.
         */
        public Builder linger(Duration linger) { this.linger = linger; return this; }

        /**
         * Maximum time to wait when the intake queue is full. {@code null} (default)
         * means block indefinitely.
         */
        public Builder maxQueueWaitTime(Duration wait) { this.maxQueueWaitTime = wait; return this; }

        /** Number of concurrent HTTP upload workers. Default: 1. */
        public Builder uploadWorkers(int workers) { this.uploadWorkers = workers; return this; }

        /** Per HTTP upload or label-poll request timeout. Default: 300 s. */
        public Builder uploadRequestTimeout(Duration timeout) {
            this.uploadRequestTimeout = timeout;
            return this;
        }

        /** Total retry budget for retriable upload outcomes. Default: 300 s. */
        public Builder uploadTimeout(Duration timeout) { this.uploadTimeout = timeout; return this; }

        /** Total polling budget after an ambiguous upload outcome. Default: 300 s. */
        public Builder statusPollTimeout(Duration timeout) {
            this.statusPollTimeout = timeout;
            return this;
        }

        /**
         * Deprecated: upload retries are governed by {@link #uploadTimeout(Duration)}.
         * This field is retained for API compatibility but has no effect.
         */
        @Deprecated
        public Builder maxRetries(int retries) { this.maxRetries = retries; return this; }

        /** Client-side record validation level. Default: {@link ValidationMode#SYNTAX}. */
        public Builder validationMode(ValidationMode mode) { this.validationMode = mode; return this; }

        /**
         * When {@code true}, records are accepted and handles completed locally without
         * making real HTTP calls. Useful for benchmarking the client pipeline.
         */
        public Builder fakeSend(boolean fake) { this.fakeSend = fake; return this; }

        /** Simulated delay per batch when {@code fakeSend} is true. Default: 500 ms. */
        public Builder fakeSendDelay(Duration delay) { this.fakeSendDelay = delay; return this; }

        /**
         * Fraction of fake batches that succeed. {@code 1.0} (default) means always succeed;
         * {@code 0.0} means always fail; {@code 0.8} means 80 % succeed. Only meaningful when
         * {@link #fakeSend(boolean)} is {@code true}.
         */
        public Builder fakeSendSuccessRate(double rate) { this.fakeSendSuccessRate = rate; return this; }

        /** Skip TLS certificate verification. Only for development/testing. */
        public Builder tlsSkipVerify(boolean skip) { this.tlsSkipVerify = skip; return this; }

        /**
         * Path to a PEM or PKCS12 CA certificate file used to verify the Doris server.
         * Ignored when {@link #tlsSkipVerify(boolean)} is {@code true}.
         */
        public Builder tlsCaCertPath(String path) { this.tlsCaCertPath = path; return this; }

        /** Prefix prepended to generated batch labels. Default: {@code java_stream_load}. */
        public Builder labelPrefix(String prefix) { this.labelPrefix = prefix; return this; }

        /** Optional custom Java {@link HttpClient}. Redirects are still handled by the SDK. */
        public Builder httpClient(HttpClient httpClient) { this.httpClient = httpClient; return this; }

        /** Log callbacks slower than this duration. Default: 10 ms. */
        public Builder slowCallbackWarn(Duration warn) { this.slowCallbackWarn = warn; return this; }

        /** Add an extra HTTP header (single value) sent with every stream-load request. */
        public Builder customHeader(String name, String value) {
            this.customHeaders.computeIfAbsent(name, k -> new ArrayList<>()).add(value);
            return this;
        }

        /** Add an extra HTTP header (multiple values) sent with every stream-load request. */
        public Builder customHeader(String name, List<String> values) {
            this.customHeaders.put(name, new ArrayList<>(values));
            return this;
        }

        public StreamLoadConfig build() {
            validate();
            return new StreamLoadConfig(this);
        }

        private void validate() {
            if (columns == null || columns.isEmpty())
                throw new IllegalArgumentException("columns must be configured");
            if (streamLoadUrl == null || streamLoadUrl.isBlank()) {
                if (endpoint == null || endpoint.isBlank())
                    throw new IllegalArgumentException("endpoint is required when streamLoadUrl is not set");
                validateEndpointUrl(endpoint);
                if (database == null || database.isBlank())
                    throw new IllegalArgumentException("database is required when streamLoadUrl is not set");
                if (table == null || table.isBlank())
                    throw new IllegalArgumentException("table is required when streamLoadUrl is not set");
            } else {
                validateStreamLoadUrl(streamLoadUrl);
            }
            if (uploadWorkers <= 0)
                throw new IllegalArgumentException("uploadWorkers must be > 0");
            if (maxUploadQueueSize <= 0)
                throw new IllegalArgumentException("maxUploadQueueSize must be > 0");
            if (batchBytes <= 0)
                throw new IllegalArgumentException("batchBytes must be > 0");
            if (batchBytes > 90L * 1024 * 1024)
                throw new IllegalArgumentException("batchBytes must be <= 90 MiB");
            if (maxQueueSize <= 0)
                throw new IllegalArgumentException("maxQueueSize must be > 0");
            if (maxQueueWaitTime != null && maxQueueWaitTime.isNegative())
                throw new IllegalArgumentException("maxQueueWaitTime must be >= 0");
            if (linger == null || linger.isNegative())
                throw new IllegalArgumentException("linger must be >= 0");
            if (uploadRequestTimeout == null || uploadRequestTimeout.compareTo(Duration.ofSeconds(10)) < 0)
                throw new IllegalArgumentException("uploadRequestTimeout must be >= 10s");
            if (uploadTimeout == null || uploadTimeout.isZero() || uploadTimeout.isNegative())
                throw new IllegalArgumentException("uploadTimeout must be > 0");
            if (statusPollTimeout == null || statusPollTimeout.isZero() || statusPollTimeout.isNegative())
                throw new IllegalArgumentException("statusPollTimeout must be > 0");
            if (fakeSendDelay == null || fakeSendDelay.isNegative())
                throw new IllegalArgumentException("fakeSendDelay must be >= 0");
            if (fakeSendSuccessRate < 0.0 || fakeSendSuccessRate > 1.0)
                throw new IllegalArgumentException("fakeSendSuccessRate must be between 0.0 and 1.0");
            if (labelPrefix == null || labelPrefix.isBlank())
                throw new IllegalArgumentException("labelPrefix cannot be empty");
            if (slowCallbackWarn == null || slowCallbackWarn.isNegative())
                throw new IllegalArgumentException("slowCallbackWarn must be >= 0");
            if (mode == null)
                throw new IllegalArgumentException("mode is required");
            if (validationMode == null)
                throw new IllegalArgumentException("validationMode is required");
            if (csvSeparator == null || csvSeparator.isEmpty())
                throw new IllegalArgumentException("csvSeparator must not be empty");
            if (csvSeparator.indexOf('\n') >= 0 || csvSeparator.indexOf('\r') >= 0)
                throw new IllegalArgumentException("csvSeparator must not contain line breaks");
            if (csvEnclose != null && !csvEnclose.isEmpty() && csvEnclose.length() != 1)
                throw new IllegalArgumentException("csvEnclose must be empty or a single character");
        }

        private URI parseHttpUrl(String raw, String field) {
            URI uri;
            try {
                uri = URI.create(raw.trim());
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(field + " must be a valid URL", e);
            }
            String scheme = uri.getScheme();
            if (!"http".equalsIgnoreCase(scheme) && !"https".equalsIgnoreCase(scheme)) {
                throw new IllegalArgumentException(field + " must use http or https");
            }
            if (uri.getHost() == null || uri.getHost().isBlank()) {
                throw new IllegalArgumentException(field + " must include host[:port]");
            }
            return uri;
        }

        private void validateEndpointUrl(String raw) {
            URI uri = parseHttpUrl(raw, "endpoint");
            if (uri.getUserInfo() != null) {
                throw new IllegalArgumentException("endpoint must not include user info");
            }
            if (uri.getRawQuery() != null && !uri.getRawQuery().isBlank()) {
                throw new IllegalArgumentException("endpoint must not include query parameters");
            }
            if (uri.getFragment() != null && !uri.getFragment().isBlank()) {
                throw new IllegalArgumentException("endpoint must not include fragment");
            }
            String path = uri.getPath();
            if (path != null && !path.isEmpty() && !"/".equals(path)) {
                throw new IllegalArgumentException("endpoint must be http(s)://host[:port] with no path");
            }
        }

        private void validateStreamLoadUrl(String raw) {
            URI uri = parseHttpUrl(raw, "streamLoadUrl");
            if (uri.getFragment() != null && !uri.getFragment().isBlank()) {
                throw new IllegalArgumentException("streamLoadUrl must not include fragment");
            }
        }
    }
}
