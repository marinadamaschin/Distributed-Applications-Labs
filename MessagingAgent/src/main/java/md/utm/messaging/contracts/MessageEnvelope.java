package md.utm.messaging.contracts;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.OffsetDateTime;

public record MessageEnvelope(
    @JsonProperty("messageId") String id,
    @JsonProperty("occurredAt") OffsetDateTime timestamp,
    @JsonProperty("correlationId") String correlationId,
    @JsonProperty("schemaVersion") String schemaVersion,
    @JsonProperty("messageType") String type,
    @JsonProperty("destination") String destination,
    @JsonProperty("payload") String payload) {}
