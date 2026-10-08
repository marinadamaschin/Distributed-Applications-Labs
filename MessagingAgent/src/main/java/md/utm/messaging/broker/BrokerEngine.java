package md.utm.messaging.broker;

import md.utm.messaging.contracts.JsonLineProtocol;
import md.utm.messaging.contracts.MessageEnvelope;
import md.utm.messaging.contracts.StructuredLog;
import md.utm.messaging.persistence.DeliveryRecord;
import md.utm.messaging.persistence.PersistentMessageStore;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public final class BrokerEngine {

    private static final int BATCH_SIZE = 20;
    private static final long POLL_INTERVAL_MS = 100;

    private final PersistentMessageStore store;
    private final ConsumerRegistry registry;
    private final BrokerOptions options;

    private final Set<SubscriberKey> dispatchers =
            ConcurrentHashMap.newKeySet();

    private final ExecutorService pool =
            Executors.newCachedThreadPool();

    private volatile boolean running = true;

    public BrokerEngine(
            PersistentMessageStore store,
            ConsumerRegistry registry,
            BrokerOptions options) {

        this.store = store;
        this.registry = registry;
        this.options = options;
    }

    public boolean publish(MessageEnvelope message)
            throws SQLException, IOException {

        if (!valid(message)) {
            return false;
        }

        String json = JsonLineProtocol.mapper()
                .writeValueAsString(message);

        var result = store.saveMessage(
                message.id(),
                message.destination(),
                json,
                message.timestamp().toInstant().toString()
        );

        StructuredLog.write(
                "INFO",
                "broker",
                "ACCEPTED",
                message,
                result.name(),
                null
        );

        // Mesajul este deja salvat in SQLite.
        // Problemele de pornire a livrarii nu trebuie
        // confundate cu esecul persistentei.
        for (ConsumerConnection connection :
                registry.getAll(message.destination())) {

            try {
                startDispatcher(
                        message.destination(),
                        connection.consumerId()
                );
            } catch (RuntimeException e) {
                System.err.println(
                        "[BROKER] Message persisted, but dispatcher "
                                + "could not start for subscriber "
                                + connection.consumerId()
                                + ": " + e.getMessage()
                );
            }
        }

        return true;
    }

    public void subscribe(
            String topic,
            String subscriberId) throws SQLException {

        store.subscribe(topic, subscriberId);
    }

    public void subscriberConnected(
            String topic,
            String subscriberId) {

        startDispatcher(topic, subscriberId);
    }

    private boolean valid(MessageEnvelope message) {
        return message != null
                && message.id() != null
                && !message.id().isBlank()
                && message.timestamp() != null
                && message.correlationId() != null
                && "1.0".equals(message.schemaVersion())
                && message.type() != null
                && message.destination() != null
                && !message.destination().isBlank()
                && message.payload() != null;
    }

    private void startDispatcher(
            String topic,
            String subscriberId) {

        if (!running) {
            return;
        }

        SubscriberKey key =
                new SubscriberKey(topic, subscriberId);

        if (dispatchers.add(key)) {
            try {
                pool.submit(() -> processSubscriber(key));
            } catch (RuntimeException e) {
                dispatchers.remove(key);
                throw e;
            }
        }
    }

    private void processSubscriber(SubscriberKey key) {
        try {
            while (running
                    && !Thread.currentThread().isInterrupted()) {

                ConsumerConnection connection = registry.get(
                        key.topic(), key.subscriberId());

                if (connection == null) {
                    return;
                }

                List<DeliveryRecord> ready =
                        store.findReadyDeliveries(
                                key.topic(),
                                key.subscriberId(),
                                BATCH_SIZE);

                if (ready.isEmpty()) {
                    Thread.sleep(POLL_INTERVAL_MS);
                    continue;
                }

                for (DeliveryRecord delivery : ready) {
                    if (!running
                            || Thread.currentThread().isInterrupted()) {
                        return;
                    }

                    // O reconectare poate inlocui conexiunea.
                    ConsumerConnection current = registry.get(
                            key.topic(), key.subscriberId());

                    if (current == null) {
                        return;
                    }

                    processDelivery(delivery, current);
                }
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

        } catch (SQLException e) {
            System.err.println(
                    "Persistence error for subscriber "
                            + key.subscriberId() + ": "
                            + e.getMessage());

        } finally {
            dispatchers.remove(key);

            // Daca subscriber-ul s-a reconectat intre timp,
            // pornim un nou dispatcher pentru conexiunea activa.
            if (running && registry.get(
                    key.topic(), key.subscriberId()) != null) {

                startDispatcher(
                        key.topic(), key.subscriberId());
            }
        }
    }

    private void processDelivery(
            DeliveryRecord delivery,
            ConsumerConnection connection) throws SQLException {

        String messageId = delivery.messageId();
        String subscriberId = delivery.subscriberId();

        if (!store.markInFlight(messageId, subscriberId)) {
            return;
        }

        int attempt = delivery.attempts() + 1;
        MessageEnvelope message = null;

        try {
            message = JsonLineProtocol.mapper().readValue(
                    delivery.envelopeJson(),
                    MessageEnvelope.class);

            StructuredLog.write(
                    "INFO", "broker", "DELIVERY",
                    message, null,
                    "attempt=" + attempt
                            + " consumer=" + subscriberId);

            boolean acknowledged =
                    connection.deliver(message, attempt);


            if (acknowledged) {

                boolean saved = store.markAcknowledged(
                        messageId, subscriberId);

                if (!saved) {
                    // Livrarea poate fi deja CANCELLED
                    // din cauza unei dezabonari.
                    System.out.println(
                            "[BROKER] ACK ignored because delivery "
                                    + "is no longer IN_FLIGHT: "
                                    + messageId);
                    return;
                }

                StructuredLog.write(
                        "INFO", "broker", "CONFIRMED",
                        message, "success",
                        "attempt=" + attempt
                                + " consumer=" + subscriberId);

                return;
            }


            handleFailure(
                    delivery, attempt, "Subscriber NACK");

        } catch (IOException e) {
            // ACK timeout, conexiune pierduta sau mesaj invalid.
            registry.removeIfSame(
                    delivery.topic(), connection);

            try {
                connection.close();
            } catch (IOException ignored) {
            }

            handleFailure(
                    delivery,
                    attempt,
                    e.getClass().getSimpleName()
                            + ": " + e.getMessage());
        }
    }


    private void handleFailure(
            DeliveryRecord delivery,
            int attempt,
            String reason) throws SQLException {

        if (attempt >= options.maxAttempts()) {

            boolean saved = store.markDeadLetter(
                    delivery.messageId(),
                    delivery.subscriberId(),
                    reason);

            if (!saved) {
                System.out.println(
                        "[BROKER] DLQ update skipped; "
                                + "delivery is no longer IN_FLIGHT: "
                                + delivery.messageId());
                return;
            }

            System.err.println(
                    "[DLQ] message=" + delivery.messageId()
                            + " subscriber=" + delivery.subscriberId()
                            + " attempts=" + attempt);

        } else {

            boolean saved = store.scheduleRetry(
                    delivery.messageId(),
                    delivery.subscriberId(),
                    reason,
                    options.retryDelayMilliseconds());

            if (!saved) {
                System.out.println(
                        "[BROKER] Retry skipped; "
                                + "delivery is no longer IN_FLIGHT: "
                                + delivery.messageId());
            }
        }
    }


    public void shutdown() {
        running = false;
        pool.shutdownNow();

        try {
            if (!pool.awaitTermination(6, TimeUnit.SECONDS)) {
                System.err.println(
                        "Broker dispatchers did not stop in time");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void unsubscribe(
            String topic,
            String subscriberId) throws SQLException {

        store.unsubscribe(topic, subscriberId);
    }

    private record SubscriberKey(
            String topic,
            String subscriberId) {
    }
}
