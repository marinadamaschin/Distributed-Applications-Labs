package md.utm.messaging.persistence;

import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public final class BackupScheduler implements AutoCloseable {

    private final PersistentMessageStore store;
    private final Path backupDirectory;
    private final Duration interval;

    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread thread = new Thread(r, "sqlite-backup");
                thread.setDaemon(true);
                return thread;
            });

    private boolean started = false;

    public BackupScheduler(
            PersistentMessageStore store,
            Path backupDirectory,
            Duration interval) {

        if (store == null || backupDirectory == null ||
                interval == null || interval.isZero() ||
                interval.isNegative() || interval.toMillis() == 0) {
            throw new IllegalArgumentException(
                    "Invalid backup scheduler configuration");
        }

        this.store = store;
        this.backupDirectory = backupDirectory;
        this.interval = interval;
    }

    public synchronized void start() {

        if (started) {
            throw new IllegalStateException(
                    "Backup scheduler already started");
        }

        started = true;

        long periodMillis = interval.toMillis();

        scheduler.scheduleWithFixedDelay(
                this::performBackup,
                periodMillis,
                periodMillis,
                TimeUnit.MILLISECONDS
        );

        System.out.println(
                "[BACKUP] Automatic backup enabled. Interval: "
                        + periodMillis + " ms");
    }

    private void performBackup() {

        try {
            Path backup = store.createBackup(backupDirectory);

            System.out.println(
                    "[BACKUP] Created successfully: " + backup);

        } catch (Exception e) {
            System.err.println(
                    "[BACKUP] Failed: " + e.getMessage());
        }
    }

    @Override
    public void close() {

        scheduler.shutdown();

        try {
            if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
