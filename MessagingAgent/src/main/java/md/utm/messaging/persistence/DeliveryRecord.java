package md.utm.messaging.persistence;

public record DeliveryRecord(
        String messageId,
        String subscriberId,
        String topic,
        String envelopeJson,
        String status,
        int attempts,
        String lastError,
        String nextRetryAt
) {
}