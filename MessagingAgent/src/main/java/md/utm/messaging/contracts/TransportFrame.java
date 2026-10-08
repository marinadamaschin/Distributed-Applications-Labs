package md.utm.messaging.contracts;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record TransportFrame(String kind, MessageEnvelope message, String messageId, String consumerId,
                             String destination, Boolean success, String reason, Integer attempt) {
    public static TransportFrame publish(MessageEnvelope m) {
        return new TransportFrame("publish", m, null, null, null, null, null, null);
    }

    public static TransportFrame accepted(String id) {
        return new TransportFrame("publishAccepted", null, id, null, null, true, null, null);
    }

    public static TransportFrame register(String cid, String dest) {
        return new TransportFrame("registerConsumer", null, null, cid, dest, null, null, null);
    }

    public static TransportFrame registered(String cid, String dest) {
        return new TransportFrame("registered", null, null, cid, dest, true, null, null);
    }

    public static TransportFrame delivery(MessageEnvelope m, int a) {
        return new TransportFrame("delivery", m, m.id(), null, m.destination(), null, null, a);
    }

    public static TransportFrame ack(String id) {
        return new TransportFrame("ack", null, id, null, null, true, null, null);
    }

    public static TransportFrame nack(String id, String reason) {
        return new TransportFrame("nack", null, id, null, null, false, reason, null);
    }

    public static TransportFrame error(String reason) {
        return new TransportFrame("error", null, null, null, null, false, reason, null);
    }

    public static TransportFrame unsubscribe(
            String consumerId,
            String destination) {

        return new TransportFrame(
                "unsubscribe",
                null,
                null,
                consumerId,
                destination,
                null,
                null,
                null
        );
    }

    public static TransportFrame unsubscribed(
            String consumerId,
            String destination) {

        return new TransportFrame(
                "unsubscribed",
                null,
                null,
                consumerId,
                destination,
                true,
                null,
                null
        );
    }

}
