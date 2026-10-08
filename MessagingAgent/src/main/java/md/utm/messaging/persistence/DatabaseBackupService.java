package md.utm.messaging.persistence;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.format.DateTimeFormatter;

public final class DatabaseBackupService {

    private final SQLiteDatabase database;
    private final Path backupDirectory;

    public DatabaseBackupService(
            SQLiteDatabase database,
            Path backupDirectory) {

        this.database = database;
        this.backupDirectory = backupDirectory;
    }

    public Path createBackup() throws SQLException, IOException {

        Files.createDirectories(backupDirectory);

        String timestamp = DateTimeFormatter
                .ofPattern("yyyyMMdd-HHmmss-SSS")
                .withZone(java.time.ZoneOffset.UTC)
                .format(Instant.now());

        Path backupPath = backupDirectory
                .resolve("broker-backup-" + timestamp
                        + "-" + java.util.UUID.randomUUID() + ".db")
                .toAbsolutePath();

        // Calea este generata intern, nu primita ca SQL de la client.
        String escapedPath = backupPath.toString()
                .replace("'", "''");

        String sql = "VACUUM INTO '" + escapedPath + "'";

        try (Connection connection = database.openConnection();
             Statement statement = connection.createStatement()) {

            statement.execute(sql);
        }

        return backupPath;
    }
}

