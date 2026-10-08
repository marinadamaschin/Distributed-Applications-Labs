package md.utm.messaging.orders;

import md.utm.messaging.contracts.*;

public final class OrderMessageBuilder {
    private OrderMessageBuilder() {
    }

    public static MessageEnvelope order(String payload, String correlationId) {
        return MessageFactory.create("OrderCreated", "orders", payload, correlationId);
    }

    public static MessageEnvelope audit(String payload, String correlationId) {
        return MessageFactory.create("OrderAudit", "audit", payload, correlationId);
    }
}
