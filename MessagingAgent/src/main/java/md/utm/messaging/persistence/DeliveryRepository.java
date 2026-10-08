package md.utm.messaging.persistence;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public final class DeliveryRepository {

    public int createPendingDeliveries(
            Connection connection,
            String messageId,
            String topic) throws SQLException {

        String sql = """
                INSERT INTO deliveries (
                    message_id,
                    subscriber_id,
                    topic,
                    status,
                    attempts
                )
                SELECT ?, subscriber_id, topic,
                       'PENDING', 0
                FROM subscriptions
                WHERE topic = ?
                  AND active = 1
                """;

        try (PreparedStatement statement =
                     connection.prepareStatement(sql)) {

            statement.setString(1, messageId);
            statement.setString(2, topic);

            return statement.executeUpdate();
        }
    }

    public List<DeliveryRecord> findReadyDeliveries(
            Connection connection,
            String topic,
            String subscriberId,
            String now,
            int limit) throws SQLException {

        String sql = """
                SELECT d.message_id,
                       d.subscriber_id,
                       d.topic,
                       m.envelope_json,
                       d.status,
                       d.attempts,
                       d.last_error,
                       d.next_retry_at
                FROM deliveries d
                JOIN messages m
                  ON m.message_id = d.message_id
                JOIN subscriptions s
                  ON s.topic = d.topic
                 AND s.subscriber_id = d.subscriber_id
                WHERE d.topic = ?
                  AND d.subscriber_id = ?
                  AND s.active = 1
                  AND (
                      d.status = 'PENDING'
                      OR (
                          d.status = 'RETRY_PENDING'
                          AND (
                              d.next_retry_at IS NULL
                              OR d.next_retry_at <= ?
                          )
                      )
                  )
                ORDER BY m.created_at, d.message_id
                LIMIT ?
                """;

        List<DeliveryRecord> deliveries = new ArrayList<>();

        try (PreparedStatement statement =
                     connection.prepareStatement(sql)) {

            statement.setString(1, topic);
            statement.setString(2, subscriberId);
            statement.setString(3, now);
            statement.setInt(4, limit);

            try (ResultSet result = statement.executeQuery()) {
                while (result.next()) {
                    deliveries.add(new DeliveryRecord(
                            result.getString("message_id"),
                            result.getString("subscriber_id"),
                            result.getString("topic"),
                            result.getString("envelope_json"),
                            result.getString("status"),
                            result.getInt("attempts"),
                            result.getString("last_error"),
                            result.getString("next_retry_at")
                    ));
                }
            }
        }

        return deliveries;
    }

    public boolean markInFlight(
            Connection connection,
            String messageId,
            String subscriberId,
            String now) throws SQLException {

        String sql = """
                UPDATE deliveries
                SET status = 'IN_FLIGHT',
                    attempts = attempts + 1,
                    last_error = NULL,
                    next_retry_at = NULL
                WHERE message_id = ?
                  AND subscriber_id = ?
                  AND (
                      status = 'PENDING'
                      OR (
                          status = 'RETRY_PENDING'
                          AND (
                              next_retry_at IS NULL
                              OR next_retry_at <= ?
                          )
                      )
                  )
                """;

        try (PreparedStatement statement =
                     connection.prepareStatement(sql)) {

            statement.setString(1, messageId);
            statement.setString(2, subscriberId);
            statement.setString(3, now);

            return statement.executeUpdate() == 1;
        }
    }

    public boolean markAcknowledged(
            Connection connection,
            String messageId,
            String subscriberId) throws SQLException {

        String sql = """
                UPDATE deliveries
                SET status = 'ACKED',
                    last_error = NULL,
                    next_retry_at = NULL
                WHERE message_id = ?
                  AND subscriber_id = ?
                  AND status = 'IN_FLIGHT'
                """;

        try (PreparedStatement statement =
                     connection.prepareStatement(sql)) {

            statement.setString(1, messageId);
            statement.setString(2, subscriberId);

            return statement.executeUpdate() == 1;
        }
    }

    public boolean scheduleRetry(
            Connection connection,
            String messageId,
            String subscriberId,
            String error,
            String nextRetryAt) throws SQLException {

        String sql = """
                UPDATE deliveries
                SET status = 'RETRY_PENDING',
                    last_error = ?,
                    next_retry_at = ?
                WHERE message_id = ?
                  AND subscriber_id = ?
                  AND status = 'IN_FLIGHT'
                """;

        try (PreparedStatement statement =
                     connection.prepareStatement(sql)) {

            statement.setString(1, error);
            statement.setString(2, nextRetryAt);
            statement.setString(3, messageId);
            statement.setString(4, subscriberId);

            return statement.executeUpdate() == 1;
        }
    }

    public boolean markDeadLetter(
            Connection connection,
            String messageId,
            String subscriberId,
            String reason) throws SQLException {

        String sql = """
                UPDATE deliveries
                SET status = 'DEAD_LETTER',
                    last_error = ?,
                    next_retry_at = NULL
                WHERE message_id = ?
                  AND subscriber_id = ?
                  AND status = 'IN_FLIGHT'
                """;

        try (PreparedStatement statement =
                     connection.prepareStatement(sql)) {

            statement.setString(1, reason);
            statement.setString(2, messageId);
            statement.setString(3, subscriberId);

            return statement.executeUpdate() == 1;
        }
    }

    public int recoverInFlightDeliveries(
            Connection connection) throws SQLException {

        String sql = """
                UPDATE deliveries
                SET status = 'RETRY_PENDING',
                    last_error = 'Broker restarted before ACK',
                    next_retry_at = NULL
                WHERE status = 'IN_FLIGHT'
                """;

        try (PreparedStatement statement =
                     connection.prepareStatement(sql)) {

            return statement.executeUpdate();
        }
    }
}

