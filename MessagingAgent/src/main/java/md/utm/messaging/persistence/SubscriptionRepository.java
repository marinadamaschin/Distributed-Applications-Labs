package md.utm.messaging.persistence;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;

public final class SubscriptionRepository {

    public void subscribe(
            Connection connection,
            String topic,
            String subscriberId) throws SQLException {

        String sql = """
                INSERT INTO subscriptions (
                    topic, subscriber_id, active, created_at
                )
                VALUES (?, ?, 1, ?)
                ON CONFLICT(topic, subscriber_id)
                DO UPDATE SET active = 1
                """;

        try (PreparedStatement statement =
                     connection.prepareStatement(sql)) {

            statement.setString(1, topic);
            statement.setString(2, subscriberId);
            statement.setString(3, Instant.now().toString());
            statement.executeUpdate();
        }
    }

    public void unsubscribe(
            Connection connection,
            String topic,
            String subscriberId) throws SQLException {

        String sql = """
                UPDATE subscriptions
                SET active = 0
                WHERE topic = ? AND subscriber_id = ?
                """;

        try (PreparedStatement statement =
                     connection.prepareStatement(sql)) {

            statement.setString(1, topic);
            statement.setString(2, subscriberId);
            statement.executeUpdate();
        }
    }
}
