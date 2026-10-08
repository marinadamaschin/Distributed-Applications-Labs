package md.utm.messaging.persistence;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public final class DatabaseRestoreService {

    public Path restore(
            Path backupPath,
            Path destinationPath) throws IOException, SQLException {

        Path source = backupPath.toAbsolutePath().normalize();
        Path destination = destinationPath.toAbsolutePath().normalize();

        if (!Files.isRegularFile(source)) {
            throw new IOException("Backup file does not exist: " + source);
        }

        if (Files.exists(destination)) {
            throw new IOException(
                    "Restore destination already exists: " + destination);
        }

        // Verificam integritatea inainte de copiere.
        validateDatabase(source);

        Path parent = destination.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }

        try {
            Files.copy(source, destination);

            // Verificam si copia restaurata.
            validateDatabase(destination);

            return destination;

        } catch (IOException | SQLException e) {
            try {
                Files.deleteIfExists(destination);
            } catch (IOException cleanupError) {
                e.addSuppressed(cleanupError);
            }
            throw e;
        }
    }

    private void validateDatabase(Path path) throws SQLException {

        String url = "jdbc:sqlite:file:"
                + path.toUri().getRawPath()
                + "?mode=ro";

        try (Connection connection = DriverManager.getConnection(url);
             Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery(
                     "PRAGMA integrity_check")) {

            if (!result.next() ||
                    !"ok".equalsIgnoreCase(result.getString(1))) {

                throw new SQLException(
                        "SQLite integrity check failed: " + path);
            }
        }
    }
}
