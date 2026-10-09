package md.utm.messaging.broker;

import md.utm.messaging.persistence.BackupScheduler;
import md.utm.messaging.persistence.PersistentMessageStore;

import java.nio.file.Path;
import java.time.Duration;

public final class BrokerApplication {

    public static void main(String[] args) throws Exception {

        int port = Integer.parseInt(
                System.getenv().getOrDefault(
                        "BROKER_PORT", "5000"));

        Path databasePath = Path.of(
                System.getenv().getOrDefault(
                        "BROKER_DB_PATH", "data/broker.db"));

        Path backupDirectory = Path.of(
                System.getenv().getOrDefault(
                        "BROKER_BACKUP_DIR", "data/backups"));

        long backupIntervalSeconds = Long.parseLong(
                System.getenv().getOrDefault(
                        "BROKER_BACKUP_INTERVAL_SECONDS", "300"));

        var options = new BrokerOptions(port, 3, 300);

        var store = new PersistentMessageStore(databasePath);
        var registry = new ConsumerRegistry();

        int recovered = store.recoverInFlightDeliveries();

        System.out.println("Recovered deliveries: " + recovered);

        var engine = new BrokerEngine(store, registry, options);

        try (var server = new BrokerServer(
                options, engine, registry);
             var backupScheduler = new BackupScheduler(
                     store,
                     backupDirectory,
                     Duration.ofSeconds(backupIntervalSeconds))) {

            backupScheduler.start();
            server.run();
        }
    }
}
