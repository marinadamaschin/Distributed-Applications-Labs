package md.utm.messaging.persistence;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class PersistentMessageStoreTest {

    @TempDir
    Path tempDir;

    // Verifica daca mesajele raman salvate dupa redeschiderea bazei de date.
    @Test
    void databaseIsCreatedAndDataSurvivesReopening()
            throws Exception {

        Path databasePath =
                tempDir.resolve("broker-test.db");

        PersistentMessageStore store =
                new PersistentMessageStore(databasePath);

        try (Connection connection = store.openConnection();
             Statement statement = connection.createStatement()) {

            statement.executeUpdate("""
                    INSERT INTO messages (
                        message_id, topic, envelope_json, created_at
                    ) VALUES (
                        'msg-001',
                        'orders',
                        '{"messageId":"msg-001"}',
                        '2026-10-08T10:00:00Z'
                    )
                    """);
        }

        PersistentMessageStore reopenedStore =
                new PersistentMessageStore(databasePath);

        try (Connection connection = reopenedStore.openConnection();
             Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery(
                     "SELECT topic FROM messages " +
                             "WHERE message_id = 'msg-001'")) {

            assertTrue(result.next());
            assertEquals("orders", result.getString("topic"));
        }
    }

    // Verifica livrarile pentru toti abonatii si evitarea mesajelor duplicate.
    @Test
    void oneMessageCreatesDeliveriesForAllSubscribers()
            throws Exception {

        Path databasePath =
                tempDir.resolve("pubsub-test.db");

        PersistentMessageStore store =
                new PersistentMessageStore(databasePath);

        store.subscribe("orders", "subscriber-A");
        store.subscribe("orders", "subscriber-B");

        String json =
                "{\"messageId\":\"msg-100\",\"destination\":\"orders\"}";

        var firstResult = store.saveMessage(
                "msg-100",
                "orders",
                json,
                "2026-10-08T10:00:00Z"
        );

        assertEquals(
                PersistentMessageStore.PublishResult.CREATED,
                firstResult
        );

        var secondResult = store.saveMessage(
                "msg-100",
                "orders",
                json,
                "2026-10-08T10:00:00Z"
        );

        assertEquals(
                PersistentMessageStore.PublishResult.ALREADY_EXISTS,
                secondResult
        );

        PersistentMessageStore reopened =
                new PersistentMessageStore(databasePath);

        try (Connection connection = reopened.openConnection();
             Statement statement = connection.createStatement()) {

            try (ResultSet result = statement.executeQuery(
                    "SELECT COUNT(*) FROM messages")) {

                assertTrue(result.next());
                assertEquals(1, result.getInt(1));
            }

            try (ResultSet result = statement.executeQuery(
                    "SELECT COUNT(*) FROM deliveries " +
                            "WHERE message_id = 'msg-100' " +
                            "AND status = 'PENDING'")) {

                assertTrue(result.next());
                assertEquals(2, result.getInt(1));
            }
        }
    }

    // Verifica recuperarea unei livrari neconfirmate dupa restart.
    @Test
    void deliveryStateSurvivesRestartAndCanBeRecovered()
            throws Exception {

        Path databasePath =
                tempDir.resolve("delivery-recovery-test.db");

        PersistentMessageStore store =
                new PersistentMessageStore(databasePath);

        store.subscribe("orders", "subscriber-A");

        store.saveMessage(
                "msg-recovery",
                "orders",
                "{\"messageId\":\"msg-recovery\"}",
                "2026-10-08T10:00:00Z"
        );

        assertEquals(
                1,
                store.findReadyDeliveries(
                        "orders", "subscriber-A", 10).size()
        );

        assertTrue(
                store.markInFlight(
                        "msg-recovery", "subscriber-A")
        );

        // Simulam redeschiderea bazei dupa un restart.
        PersistentMessageStore restarted =
                new PersistentMessageStore(databasePath);

        assertEquals(
                1,
                restarted.recoverInFlightDeliveries()
        );

        assertEquals(
                1,
                restarted.findReadyDeliveries(
                        "orders", "subscriber-A", 10).size()
        );

        assertTrue(
                restarted.markInFlight(
                        "msg-recovery", "subscriber-A")
        );

        assertTrue(
                restarted.markAcknowledged(
                        "msg-recovery", "subscriber-A")
        );

        // Mesajul confirmat nu mai este disponibil.
        assertTrue(
                restarted.findReadyDeliveries(
                        "orders", "subscriber-A", 10).isEmpty()
        );
    }

    // Verifica daca fiecare abonat primeste mesajele topicului cerut.
    @Test
    void subscriberReceivesOnlyDeliveriesForRequestedTopic()
            throws Exception {

        Path databasePath =
                tempDir.resolve("topic-isolation-test.db");

        PersistentMessageStore store =
                new PersistentMessageStore(databasePath);

        store.subscribe("orders", "subscriber-A");
        store.subscribe("audit", "subscriber-A");

        store.saveMessage(
                "msg-orders",
                "orders",
                "{\"messageId\":\"msg-orders\"}",
                "2026-10-08T10:00:00Z"
        );

        store.saveMessage(
                "msg-audit",
                "audit",
                "{\"messageId\":\"msg-audit\"}",
                "2026-10-08T10:01:00Z"
        );

        var orderDeliveries =
                store.findReadyDeliveries(
                        "orders", "subscriber-A", 10);

        var auditDeliveries =
                store.findReadyDeliveries(
                        "audit", "subscriber-A", 10);

        assertEquals(1, orderDeliveries.size());
        assertEquals(
                "msg-orders",
                orderDeliveries.get(0).messageId()
        );

        assertEquals(1, auditDeliveries.size());
        assertEquals(
                "msg-audit",
                auditDeliveries.get(0).messageId()
        );
    }

    // Verifica daca starea DLQ ramane salvata dupa restart.
    @Test
    void deadLetterStateSurvivesRestart() throws Exception {

        Path databasePath =
                tempDir.resolve("dead-letter-test.db");

        PersistentMessageStore store =
                new PersistentMessageStore(databasePath);

        store.subscribe("orders", "subscriber-NACK");

        store.saveMessage(
                "msg-dlq",
                "orders",
                "{\"messageId\":\"msg-dlq\"}",
                "2026-10-08T10:00:00Z"
        );

        // Prima incercare de livrare.
        assertTrue(store.markInFlight(
                "msg-dlq", "subscriber-NACK"));

        // NACK: programam o noua incercare.
        assertTrue(store.scheduleRetry(
                "msg-dlq",
                "subscriber-NACK",
                "Simulated failure",
                0
        ));

        // A doua incercare.
        assertTrue(store.markInFlight(
                "msg-dlq", "subscriber-NACK"));

        assertTrue(store.scheduleRetry(
                "msg-dlq",
                "subscriber-NACK",
                "Simulated failure",
                0
        ));

        // A treia incercare: mesajul ajunge in DLQ.
        assertTrue(store.markInFlight(
                "msg-dlq", "subscriber-NACK"));

        assertTrue(store.markDeadLetter(
                "msg-dlq",
                "subscriber-NACK",
                "Retry limit exceeded"
        ));

        // Simulam restartul prin redeschiderea bazei.
        PersistentMessageStore restarted =
                new PersistentMessageStore(databasePath);

        try (Connection connection = restarted.openConnection();
             Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery("""
                     SELECT status, attempts, last_error
                     FROM deliveries
                     WHERE message_id = 'msg-dlq'
                       AND subscriber_id = 'subscriber-NACK'
                     """)) {

            assertTrue(result.next());

            assertEquals(
                    "DEAD_LETTER",
                    result.getString("status"));

            assertEquals(
                    3,
                    result.getInt("attempts"));

            assertEquals(
                    "Retry limit exceeded",
                    result.getString("last_error"));
        }

        // Mesajele din DLQ nu sunt livrate automat.
        assertTrue(
                restarted.findReadyDeliveries(
                        "orders", "subscriber-NACK", 10).isEmpty()
        );
    }

    // Verifica daca backup-ul contine mesajele salvate.
    @Test
    void backupContainsPersistedMessages() throws Exception {

        Path databasePath =
                tempDir.resolve("backup-source.db");

        Path backupDirectory =
                tempDir.resolve("backups");

        PersistentMessageStore store =
                new PersistentMessageStore(databasePath);

        store.subscribe("orders", "subscriber-A");

        store.saveMessage(
                "msg-backup",
                "orders",
                "{\"messageId\":\"msg-backup\"}",
                "2026-10-08T10:00:00Z"
        );

        Path backupPath = store.createBackup(backupDirectory);

        assertTrue(Files.exists(backupPath));
        assertTrue(Files.size(backupPath) > 0);

        // Deschidem copia ca pe o baza de date separata.
        PersistentMessageStore restored =
                new PersistentMessageStore(backupPath);

        try (Connection connection = restored.openConnection();
             Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery(
                     "SELECT COUNT(*) FROM messages " +
                             "WHERE message_id = 'msg-backup'")) {

            assertTrue(result.next());
            assertEquals(1, result.getInt(1));
        }

        assertEquals(
                1,
                restored.findReadyDeliveries(
                        "orders", "subscriber-A", 10).size()
        );
    }

    // Verifica daca baza de date poate fi restaurata din backup.
    @Test
    void databaseCanBeRestoredFromBackup() throws Exception {

        Path originalPath = tempDir.resolve("original.db");
        Path backupDirectory = tempDir.resolve("backups");
        Path restoredPath = tempDir.resolve("restored.db");

        PersistentMessageStore original =
                new PersistentMessageStore(originalPath);

        original.subscribe("orders", "subscriber-A");

        original.saveMessage(
                "msg-restore",
                "orders",
                "{\"messageId\":\"msg-restore\"}",
                "2026-10-08T10:00:00Z"
        );

        Path backupPath = original.createBackup(backupDirectory);

        DatabaseRestoreService restoreService =
                new DatabaseRestoreService();

        Path result = restoreService.restore(
                backupPath, restoredPath);

        assertTrue(Files.exists(result));

        PersistentMessageStore restored =
                new PersistentMessageStore(restoredPath);

        var deliveries = restored.findReadyDeliveries(
                "orders", "subscriber-A", 10);

        assertEquals(1, deliveries.size());
        assertEquals("msg-restore", deliveries.get(0).messageId());
    }

    // Verifica anularea livrarilor dupa dezabonare.
    @Test
    void unsubscribeCancelsPendingDeliveries()
            throws Exception {

        Path databasePath =
                tempDir.resolve("unsubscribe-test.db");

        PersistentMessageStore store =
                new PersistentMessageStore(databasePath);

        store.subscribe("orders", "subscriber-A");
        store.subscribe("orders", "subscriber-B");

        store.saveMessage(
                "msg-unsubscribe",
                "orders",
                "{\"messageId\":\"msg-unsubscribe\"}",
                "2026-10-08T10:00:00Z"
        );

        // Initial, ambii abonati au cate o livrare.
        assertEquals(
                1,
                store.findReadyDeliveries(
                        "orders", "subscriber-A", 10).size()
        );

        assertEquals(
                1,
                store.findReadyDeliveries(
                        "orders", "subscriber-B", 10).size()
        );

        // Dezabonam doar subscriber-A.
        store.unsubscribe("orders", "subscriber-A");

        // Livrarea lui A nu mai este disponibila.
        assertTrue(
                store.findReadyDeliveries(
                        "orders", "subscriber-A", 10).isEmpty()
        );

        // Livrarea lui B ramane disponibila.
        assertEquals(
                1,
                store.findReadyDeliveries(
                        "orders", "subscriber-B", 10).size()
        );

        // Verificam starea persistata.
        try (Connection connection = store.openConnection();
             var statement = connection.prepareStatement("""
                     SELECT status
                     FROM deliveries
                     WHERE message_id = ?
                       AND subscriber_id = ?
                     """)) {

            statement.setString(1, "msg-unsubscribe");
            statement.setString(2, "subscriber-A");

            try (ResultSet result = statement.executeQuery()) {
                assertTrue(result.next());
                assertEquals(
                        "CANCELLED",
                        result.getString("status")
                );
            }
        }
    }

    // Verifica publicarea simultana si livrarile pentru fiecare abonat.
    @Test
    void concurrentPublishersCreateDeliveriesForAllSubscribers()
            throws Exception {

        Path databasePath =
                tempDir.resolve("concurrent-publish-test.db");

        PersistentMessageStore store =
                new PersistentMessageStore(databasePath);

        // Doi abonati activi la acelasi topic.
        store.subscribe("orders", "subscriber-A");
        store.subscribe("orders", "subscriber-B");

        int publishers = 3;
        int messagesPerPublisher = 10;
        int totalMessages = publishers * messagesPerPublisher;

        var executor = Executors.newFixedThreadPool(publishers);

        try {
            var tasks = new ArrayList<Callable<Void>>();

            for (int p = 0; p < publishers; p++) {
                final int publisherId = p;

                tasks.add(() -> {
                    for (int i = 0; i < messagesPerPublisher; i++) {

                        String messageId =
                                "publisher-" + publisherId
                                        + "-message-" + i;

                        String json =
                                "{\"messageId\":\"" + messageId + "\"}";

                        var result = store.saveMessage(
                                messageId,
                                "orders",
                                json,
                                "2026-10-08T10:00:00Z"
                        );

                        assertEquals(
                                PersistentMessageStore.PublishResult.CREATED,
                                result
                        );
                    }

                    return null;
                });
            }

            // Pornim cei trei Publisher-i concurent.
            var results = executor.invokeAll(tasks);

            // Verificam inclusiv erorile din thread-uri.
            for (var result : results) {
                result.get();
            }

        } finally {
            executor.shutdown();
            assertTrue(
                    executor.awaitTermination(10, TimeUnit.SECONDS)
            );
        }

        // Verificam datele persistate in SQLite.
        try (Connection connection = store.openConnection();
             Statement statement = connection.createStatement()) {

            try (ResultSet result = statement.executeQuery(
                    "SELECT COUNT(*) FROM messages")) {

                assertTrue(result.next());
                assertEquals(totalMessages, result.getInt(1));
            }

            try (ResultSet result = statement.executeQuery(
                    "SELECT COUNT(*) FROM deliveries")) {

                assertTrue(result.next());
                assertEquals(totalMessages * 2, result.getInt(1));
            }

            try (ResultSet result = statement.executeQuery("""
                    SELECT subscriber_id, COUNT(*) AS total
                    FROM deliveries
                    GROUP BY subscriber_id
                    """)) {

                int subscribersChecked = 0;

                while (result.next()) {
                    assertEquals(
                            totalMessages,
                            result.getInt("total")
                    );
                    subscribersChecked++;
                }

                assertEquals(2, subscribersChecked);
            }
        }
    }


}
