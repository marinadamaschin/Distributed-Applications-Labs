package md.utm.messaging.contracts;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;

public final class StructuredLog {
    private StructuredLog() {
    }

    public static synchronized void write(String level, String component, String event, MessageEnvelope m, String result, String details) {
        StringBuilder s = new StringBuilder(OffsetDateTime.now(ZoneOffset.UTC).toString()).append(" | level=").append(level).append(" | component=").append(component).append(" | event=").append(event);
        if (m != null)
            s.append(" | correlationId=").append(m.correlationId()).append(" | messageId=").append(m.id()).append(" | destination=").append(m.destination());
        if (result != null && !result.isBlank()) s.append(" | result=").append(result);
        if (details != null && !details.isBlank()) s.append(" | ").append(details);
        System.out.println(s);
    }
}
