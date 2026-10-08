package md.utm.messaging.persistence;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;

public final class PersistentMessageStore {

    private final SQLiteDatabase database;
    private final MessageRepository messageRepository;
    private final SubscriptionRepository subscriptionRepository;
    private final DeliveryRepository deliveryRepository;

    public PersistentMessageStore(Path databasePath) {
        database = new SQLiteDatabase(databasePath);
        messageRepository = new MessageRepository();
        subscriptionRepository = new SubscriptionRepository();
        deliveryRepository = new DeliveryRepository();
    }

    private static void validate(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(
                    field + " must not be empty");
        }
    }

    public Connection openConnection() throws SQLException {
        return database.openConnection();
    }

    public void subscribe(String topic, String subscriberId)
            throws SQLException {

        validate(topic, "topic");
        validate(subscriberId, "subscriberId");

        try (Connection connection = database.openConnection()) {
            subscriptionRepository.subscribe(
                    connection, topic, subscriberId);
        }
    }


    public void unsubscribe(String topic, String subscriberId)
            throws SQLException {

        validate(topic, "topic");
        validate(subscriberId, "subscriberId");

        try (Connection connection = database.openConnection()) {
            connection.setAutoCommit(false);

            try {
                subscriptionRepository.unsubscribe(
                        connection, topic, subscriberId);

                String sql = """
                        UPDATE deliveries
                        SET status = 'CANCELLED',
                            next_retry_at = NULL
                        WHERE topic = ?
                          AND subscriber_id = ?
                          AND status IN (
                              'PENDING',
                              'RETRY_PENDING',
                              'IN_FLIGHT'
                          )
                        """;

                try (var statement = connection.prepareStatement(sql)) {
                    statement.setString(1, topic);
                    statement.setString(2, subscriberId);
                    statement.executeUpdate();
                }

                connection.commit();

            } catch (SQLException | RuntimeException e) {
                try {
                    connection.rollback();
                } catch (SQLException rollbackError) {
                    e.addSuppressed(rollbackError);
                }
                throw e;
            }
        }
    }


    public PublishResult saveMessage(
            String messageId,
            String topic,
            String envelopeJson,
            String createdAt) throws SQLException {

        validate(messageId, "messageId");
        validate(topic, "topic");
        validate(envelopeJson, "envelopeJson");
        validate(createdAt, "createdAt");

        try (Connection connection = database.openConnection()) {
            connection.setAutoCommit(false);

            try {
                MessageRepository.InsertResult result =
                        messageRepository.insert(
                                connection,
                                messageId,
                                topic,
                                envelopeJson,
                                createdAt);

                if (result ==
                        MessageRepository.InsertResult.ALREADY_EXISTS) {
                    connection.commit();
                    return PublishResult.ALREADY_EXISTS;
                }

                deliveryRepository.createPendingDeliveries(
                        connection, messageId, topic);

                connection.commit();
                return PublishResult.CREATED;

            } catch (SQLException | RuntimeException e) {
                try {
                    connection.rollback();
                } catch (SQLException rollbackError) {
                    e.addSuppressed(rollbackError);
                }
                throw e;
            }
        }
    }

    public List<DeliveryRecord> findReadyDeliveries(
            String topic,
            String subscriberId,
            int limit) throws SQLException {

        validate(topic, "topic");
        validate(subscriberId, "subscriberId");

        if (limit <= 0) {
            throw new IllegalArgumentException(
                    "Limit must be positive");
        }

        try (Connection connection = database.openConnection()) {
            return deliveryRepository.findReadyDeliveries(
                    connection,
                    topic,
                    subscriberId,
                    Instant.now().toString(),
                    limit);
        }
    }

    public boolean markInFlight(
            String messageId,
            String subscriberId) throws SQLException {

        try (Connection connection = database.openConnection()) {
            return deliveryRepository.markInFlight(
                    connection,
                    messageId,
                    subscriberId,
                    Instant.now().toString());
        }
    }

    public boolean markAcknowledged(
            String messageId,
            String subscriberId) throws SQLException {

        try (Connection connection = database.openConnection()) {
            return deliveryRepository.markAcknowledged(
                    connection, messageId, subscriberId);
        }
    }

    public boolean scheduleRetry(
            String messageId,
            String subscriberId,
            String error,
            long delayMillis) throws SQLException {

        if (delayMillis < 0) {
            throw new IllegalArgumentException(
                    "Retry delay must not be negative");
        }

        String nextRetryAt = Instant.now()
                .plusMillis(delayMillis)
                .toString();

        try (Connection connection = database.openConnection()) {
            return deliveryRepository.scheduleRetry(
                    connection,
                    messageId,
                    subscriberId,
                    error,
                    nextRetryAt);
        }
    }

    public boolean markDeadLetter(
            String messageId,
            String subscriberId,
            String reason) throws SQLException {

        try (Connection connection = database.openConnection()) {
            return deliveryRepository.markDeadLetter(
                    connection,
                    messageId,
                    subscriberId,
                    reason);
        }
    }

    public int recoverInFlightDeliveries() throws SQLException {
        try (Connection connection = database.openConnection()) {
            return deliveryRepository.recoverInFlightDeliveries(
                    connection);
        }
    }

    public Path createBackup(Path backupDirectory)
            throws SQLException, java.io.IOException {

        DatabaseBackupService backupService =
                new DatabaseBackupService(database, backupDirectory);

        return backupService.createBackup();
    }

    public enum PublishResult {
        CREATED,
        ALREADY_EXISTS
    }
}
