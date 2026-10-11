package com.service.grpc.broker;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import com.service.grpc.service.GrpcMessagingService;
import com.service.grpc.store.FileMessageStore;

import java.nio.file.Path;
import java.nio.file.Files;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public final class GrpcBrokerApplication {

    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(System.getenv().getOrDefault("GRPC_PORT", "50051"));

        Path journal = Path.of(System.getenv().getOrDefault("MESSAGE_JOURNAL", "data/messages.jsonl"));
        Path backupDir = Path.of(System.getenv().getOrDefault("MESSAGE_BACKUP_DIR", "data/backups"));

        long backupSeconds = Long.parseLong(System.getenv().getOrDefault("MESSAGE_BACKUP_INTERVAL_SECONDS", "300"));

        FileMessageStore store = new FileMessageStore(journal);
        String restoreFrom = System.getenv("MESSAGE_RESTORE_FROM");

        if (restoreFrom != null && !restoreFrom.isBlank()) {
            store.restore(Path.of(restoreFrom));
            System.out.println("[RESTORE] Restored journal from " + Path.of(restoreFrom).toAbsolutePath());
        }

        ScheduledExecutorService backups = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "file-backup-scheduler");
            t.setDaemon(true);
            return t;
        });

        if (backupSeconds > 0) backups.scheduleWithFixedDelay(() -> {
            try {
                Path p = store.backup(backupDir);
                System.out.println("[BACKUP] " + p);
            } catch (Exception e) {
                System.err.println("[BACKUP-ERROR] " + e.getMessage());
            }
        }, backupSeconds, backupSeconds, TimeUnit.SECONDS);

        Server server = ServerBuilder.forPort(port).addService(new GrpcMessagingService(store)).build().start();
        System.out.println("gRPC broker listening on " + port + "; file journal: " + journal.toAbsolutePath());
        System.out.println("Periodic file backups: " + backupDir.toAbsolutePath() + " every " + backupSeconds + " seconds");
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.shutdown();
            backups.shutdownNow();
        }));

        server.awaitTermination();
    }
}