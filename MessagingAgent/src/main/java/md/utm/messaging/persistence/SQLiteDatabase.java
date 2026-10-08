package md.utm.messaging.persistence;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public final class SQLiteDatabase {

    private final String databaseUrl;

    public SQLiteDatabase(Path databasePath) {
        try {
            Path absolutePath = databasePath.toAbsolutePath();
            Path parent = absolutePath.getParent();

            if (parent != null) {
                Files.createDirectories(parent);
            }

            databaseUrl = "jdbc:sqlite:" + absolutePath;
            initializeSchema();

        } catch (IOException | SQLException e) {
            throw new IllegalStateException(
                    "Cannot initialize SQLite database", e);
        }
    }

    public Connection openConnection() throws SQLException {
        Connection connection =
                DriverManager.getConnection(databaseUrl);

        try (Statement statement = connection.createStatement()) {
            statement.execute("PRAGMA foreign_keys = ON");
            statement.execute("PRAGMA busy_timeout = 5000");
        } catch (SQLException e) {
            connection.close();
            throw e;
        }

        return connection;
    }

    private void initializeSchema() throws SQLException {
        try (Connection connection = openConnection();
             Statement statement = connection.createStatement()) {

            statement.executeUpdate("""
                    CREATE TABLE IF NOT EXISTS messages (
                        message_id TEXT PRIMARY KEY,
                        topic TEXT NOT NULL,
                        envelope_json TEXT NOT NULL,
                        publisher_id TEXT,
                        created_at TEXT NOT NULL
                    )
                    """);

            statement.executeUpdate("""
                    CREATE TABLE IF NOT EXISTS subscriptions (
                        topic TEXT NOT NULL,
                        subscriber_id TEXT NOT NULL,
                        active INTEGER NOT NULL DEFAULT 1
                            CHECK (active IN (0, 1)),
                        created_at TEXT NOT NULL,
                        PRIMARY KEY (topic, subscriber_id)
                    )
                    """);

            statement.executeUpdate("""
                    CREATE TABLE IF NOT EXISTS deliveries (
                        message_id TEXT NOT NULL,
                        subscriber_id TEXT NOT NULL,
                        topic TEXT NOT NULL,
                        status TEXT NOT NULL
                            CHECK (status IN (
                                'PENDING',
                                'IN_FLIGHT',
                                'ACKED',
                                'RETRY_PENDING',
                                'DEAD_LETTER',
                                'CANCELLED'
                            )),
                        attempts INTEGER NOT NULL DEFAULT 0
                            CHECK (attempts >= 0),
                        last_error TEXT,
                        next_retry_at TEXT,
                        PRIMARY KEY (message_id, subscriber_id),
                        FOREIGN KEY (message_id)
                            REFERENCES messages(message_id),
                        FOREIGN KEY (topic, subscriber_id)
                            REFERENCES subscriptions(topic, subscriber_id)
                    )
                    """);

            statement.executeUpdate("""
                    CREATE INDEX IF NOT EXISTS idx_deliveries_status
                    ON deliveries(status, next_retry_at)
                    """);

            statement.executeUpdate("""
                    CREATE INDEX IF NOT EXISTS idx_messages_topic
                    ON messages(topic, created_at)
                    """);
        }
    }
}
