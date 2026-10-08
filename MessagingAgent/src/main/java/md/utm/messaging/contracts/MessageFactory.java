package md.utm.messaging.contracts;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.UUID;

public final class MessageFactory {
    private MessageFactory() {
    }

    public static MessageEnvelope create(String type, String destination, String payload, String correlationId) {
        return new MessageEnvelope(UUID.randomUUID().toString().replace("-", ""), OffsetDateTime.now(ZoneOffset.UTC), correlationId == null ? UUID.randomUUID().toString().replace("-", "") : correlationId, "1.0", type, destination, payload);
    }
}
