package md.utm.messaging.persistence;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;

public final class MessageRepository {

    public InsertResult insert(
            Connection connection,
            String messageId,
            String topic,
            String envelopeJson,
            String createdAt) throws SQLException {

        String sql = """
                INSERT INTO messages (
                    message_id, topic, envelope_json, created_at
                )
                VALUES (?, ?, ?, ?)
                ON CONFLICT(message_id) DO NOTHING
                """;

        try (PreparedStatement statement =
                     connection.prepareStatement(sql)) {

            statement.setString(1, messageId);
            statement.setString(2, topic);
            statement.setString(3, envelopeJson);
            statement.setString(4, createdAt);

            if (statement.executeUpdate() == 1) {
                return InsertResult.CREATED;
            }
        }

        String checkSql = """
                SELECT topic, envelope_json
                FROM messages
                WHERE message_id = ?
                """;

        try (PreparedStatement statement =
                     connection.prepareStatement(checkSql)) {

            statement.setString(1, messageId);

            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) {
                    throw new SQLException(
                            "Existing message could not be found");
                }

                boolean sameMessage =
                        Objects.equals(
                                topic, result.getString("topic"))
                                && Objects.equals(
                                envelopeJson,
                                result.getString("envelope_json"));

                if (!sameMessage) {
                    throw new IllegalArgumentException(
                            "Message ID conflict: " + messageId);
                }

                return InsertResult.ALREADY_EXISTS;
            }
        }
    }

    public enum InsertResult {
        CREATED,
        ALREADY_EXISTS
    }
}
