package net.wushilin.doris;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * JSON-serializable configuration counterpart for {@link StreamLoadConfig}.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class LoaderConfig {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public String endpoint;
    public String database;
    public String table;
    @JsonProperty("stream_load_url")
    @JsonAlias("streamLoadUrl")
    public String streamLoadUrl;

    @JsonProperty(access = JsonProperty.Access.WRITE_ONLY)
    public String username;

    @JsonProperty(access = JsonProperty.Access.WRITE_ONLY)
    public String password;

    public Mode mode;
    public List<String> columns;

    @JsonProperty("headers")
    @JsonAlias({"customHeaders", "custom_headers"})
    public Map<String, List<String>> customHeaders;

    @JsonProperty("authentication_type")
    @JsonAlias("authenticationType")
    public String authenticationType;

    @JsonProperty("authentication_token")
    @JsonAlias("authenticationToken")
    public String authenticationToken;

    @JsonProperty("csv_separator")
    @JsonAlias("csvSeparator")
    public String csvSeparator;

    @JsonProperty("csv_quote")
    @JsonAlias("csvQuote")
    public String csvQuote;

    @JsonProperty("max_queue_size")
    @JsonAlias("maxQueueSize")
    public Integer maxQueueSize;

    @JsonProperty("max_upload_queue_size")
    @JsonAlias("maxUploadQueueSize")
    public Integer maxUploadQueueSize;

    @JsonProperty("batch_bytes")
    @JsonAlias("batchBytes")
    public Long batchBytes;
    public String linger;

    @JsonProperty("max_queue_wait_time")
    @JsonAlias("maxQueueWaitTime")
    public String maxQueueWaitTime;

    @JsonProperty("doris_upload_workers")
    @JsonAlias("uploadWorkers")
    public Integer uploadWorkers;

    @JsonProperty("doris_upload_request_timeout")
    @JsonAlias("uploadRequestTimeout")
    public String uploadRequestTimeout;

    @JsonProperty("doris_upload_timeout")
    @JsonAlias("uploadTimeout")
    public String uploadTimeout;

    @JsonProperty("status_poll_timeout")
    @JsonAlias("statusPollTimeout")
    public String statusPollTimeout;

    @JsonProperty("validation")
    @JsonAlias("validationMode")
    public ValidationMode validationMode;

    @JsonProperty("fake_send")
    @JsonAlias("fakeSend")
    public Boolean fakeSend;

    @JsonProperty("fake_send_delay")
    @JsonAlias("fakeSendDelay")
    public String fakeSendDelay;

    @JsonProperty("fake_send_success_rate")
    @JsonAlias("fakeSendSuccessRate")
    public Double fakeSendSuccessRate;

    @JsonProperty("tls_skip_verify")
    @JsonAlias("tlsSkipVerify")
    public Boolean tlsSkipVerify;

    @JsonProperty("tls_ca_cert_path")
    @JsonAlias("tlsCaCertPath")
    public String tlsCaCertPath;

    @JsonProperty("label_prefix")
    @JsonAlias("labelPrefix")
    public String labelPrefix;

    @JsonProperty("slow_callback_warn")
    @JsonAlias("slowCallbackWarn")
    public String slowCallbackWarn;

    public static LoaderConfig load(Path path) throws IOException {
        return MAPPER.readValue(path.toFile(), LoaderConfig.class);
    }

    public void save(Path path) throws IOException {
        MAPPER.writerWithDefaultPrettyPrinter().writeValue(path.toFile(), this);
    }

    public StreamLoadConfig toConfig() {
        StreamLoadConfig.Builder builder = StreamLoadConfig.builder();
        if (endpoint != null) builder.endpoint(endpoint);
        if (database != null) builder.database(database);
        if (table != null) builder.table(table);
        if (streamLoadUrl != null) builder.streamLoadUrl(streamLoadUrl);
        if ("basic".equalsIgnoreCase(authenticationType) && authenticationToken != null) {
            String[] parts = authenticationToken.split(":", 2);
            if (parts.length == 2) {
                builder.basicAuth(parts[0], parts[1]);
            }
        }
        if (username != null) builder.username(username);
        if (password != null) builder.password(password);
        if (mode != null) builder.mode(mode);
        if (columns != null) builder.columns(columns);
        if (csvSeparator != null) builder.csvSeparator(csvSeparator);
        if (csvQuote != null) builder.csvQuote(csvQuote);
        if (maxQueueSize != null) builder.maxQueueSize(maxQueueSize);
        if (maxUploadQueueSize != null) builder.maxUploadQueueSize(maxUploadQueueSize);
        if (batchBytes != null) builder.batchBytes(batchBytes);
        if (linger != null) builder.linger(parseDuration(linger));
        if (maxQueueWaitTime != null) builder.maxQueueWaitTime(parseDuration(maxQueueWaitTime));
        if (uploadWorkers != null) builder.uploadWorkers(uploadWorkers);
        if (uploadRequestTimeout != null) builder.uploadRequestTimeout(parseDuration(uploadRequestTimeout));
        if (uploadTimeout != null) builder.uploadTimeout(parseDuration(uploadTimeout));
        if (statusPollTimeout != null) builder.statusPollTimeout(parseDuration(statusPollTimeout));
        if (validationMode != null) builder.validationMode(validationMode);
        if (fakeSend != null) builder.fakeSend(fakeSend);
        if (fakeSendDelay != null) builder.fakeSendDelay(parseDuration(fakeSendDelay));
        if (fakeSendSuccessRate != null) builder.fakeSendSuccessRate(fakeSendSuccessRate);
        if (tlsSkipVerify != null) builder.tlsSkipVerify(tlsSkipVerify);
        if (tlsCaCertPath != null) builder.tlsCaCertPath(tlsCaCertPath);
        if (labelPrefix != null) builder.labelPrefix(labelPrefix);
        if (slowCallbackWarn != null) builder.slowCallbackWarn(parseDuration(slowCallbackWarn));
        if (customHeaders != null) {
            customHeaders.forEach(builder::customHeader);
        }
        return builder.build();
    }

    static Duration parseDuration(String raw) {
        String trimmed = raw.trim();
        String value = trimmed.toLowerCase();
        if (value.endsWith("ms")) {
            return Duration.ofMillis(Long.parseLong(value.substring(0, value.length() - 2)));
        }
        if (value.endsWith("s")) {
            return Duration.ofSeconds(Long.parseLong(value.substring(0, value.length() - 1)));
        }
        if (value.endsWith("m")) {
            return Duration.ofMinutes(Long.parseLong(value.substring(0, value.length() - 1)));
        }
        if (value.endsWith("h")) {
            return Duration.ofHours(Long.parseLong(value.substring(0, value.length() - 1)));
        }
        return Duration.parse(raw);
    }
}
