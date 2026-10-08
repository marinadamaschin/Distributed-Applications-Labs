package md.utm.messaging.integration;

import md.utm.messaging.broker.*;
import md.utm.messaging.contracts.*;
import md.utm.messaging.producer.*;
import org.junit.jupiter.api.*;
import md.utm.messaging.persistence.PersistentMessageStore;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import md.utm.messaging.contracts.JsonLineProtocol;
import md.utm.messaging.contracts.TransportFrame;

import static org.junit.jupiter.api.Assertions.*;

import java.util.concurrent.*;

class PublishAcceptanceTest {
    @TempDir
    Path tempDir;

    // Verifica daca Producer-ul primeste confirmarea fara ACK de la Consumer.
    @Test
    void producerGetsAcceptanceWithoutConsumerAck() throws Exception {
        int port = 15101;
        var o = new BrokerOptions(port, 2, 50);
        var store = new PersistentMessageStore(
                tempDir.resolve("publish-acceptance-test.db")
        );

        var registry = new ConsumerRegistry();

        var engine = new BrokerEngine(store, registry, o);
        var server = new BrokerServer(o, engine, registry);
        var ex = Executors.newSingleThreadExecutor();
        try {
            ex.submit(() -> {
                try {
                    server.run();
                } catch (Exception ignored) {
                }
            });
            Thread.sleep(150);
            var producer = new ProducerClient("127.0.0.1", port);
            var m = MessageFactory.create("Test", "nobody", "{}", "corr-test");
            assertTrue(producer.publish(m));
        } finally {
            server.close();
            ex.shutdownNow();
        }
    }

    // Verifica daca acelasi mesaj este salvat o singura data.
    @Test
    void repeatedPublishIsAcceptedWithoutDuplicatePersistence()
            throws Exception {

        int port = 15102;
        var options = new BrokerOptions(port, 2, 50);

        var store = new PersistentMessageStore(
                tempDir.resolve("duplicate-publish-test.db")
        );

        store.subscribe("orders", "subscriber-A");
        store.subscribe("orders", "subscriber-B");

        var registry = new ConsumerRegistry();
        var engine = new BrokerEngine(store, registry, options);
        var server = new BrokerServer(options, engine, registry);
        var executor = Executors.newSingleThreadExecutor();

        try {
            var serverTask = executor.submit(() -> {
                try {
                    server.run();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

            Thread.sleep(150);

            var producer = new ProducerClient("127.0.0.1", port);

            var message = MessageFactory.create(
                    "Test",
                    "orders",
                    "{}",
                    "corr-duplicate-test"
            );

            // Prima publicare.
            assertTrue(producer.publish(message));

            // Retransmitem exact acelasi obiect,
            // pastrand acelasi messageId.
            assertTrue(producer.publish(message));

            // Verificam persistenta dupa ambele confirmari.
            try (var connection = store.openConnection();
                 var statement = connection.prepareStatement(
                         "SELECT COUNT(*) FROM messages WHERE message_id = ?"
                 )) {

                statement.setString(1, message.id());

                try (var result = statement.executeQuery()) {
                    assertTrue(result.next());
                    assertEquals(1, result.getInt(1));
                }
            }

            // Un mesaj trebuie sa produca exact doua
            // livrari: cate una pentru fiecare abonat.
            try (var connection = store.openConnection();
                 var statement = connection.prepareStatement(
                         "SELECT COUNT(*) FROM deliveries WHERE message_id = ?"
                 )) {

                statement.setString(1, message.id());

                try (var result = statement.executeQuery()) {
                    assertTrue(result.next());
                    assertEquals(2, result.getInt(1));
                }
            }

            assertFalse(serverTask.isDone(),
                    "Broker stopped unexpectedly");

        } finally {
            server.close();
            executor.shutdownNow();
        }
    }

    // Verifica dezabonarea unui Consumer prin TCP.
    @Test
    void subscriberCanUnsubscribeThroughTcp() throws Exception {

        int port = 15103;

        var options = new BrokerOptions(port, 2, 50);

        var store = new PersistentMessageStore(
                tempDir.resolve("tcp-unsubscribe-test.db"));

        store.subscribe("orders", "subscriber-A");

        var registry = new ConsumerRegistry();
        var engine = new BrokerEngine(store, registry, options);
        var server = new BrokerServer(options, engine, registry);

        var executor = Executors.newSingleThreadExecutor();

        try {
            executor.submit(() -> {
                try {
                    server.run();
                } catch (Exception ignored) {
                }
            });

            Thread.sleep(150);

            try (var socket = new java.net.Socket(
                    "127.0.0.1", port)) {

                socket.setSoTimeout(3000);

                var reader = JsonLineProtocol.reader(
                        socket.getInputStream());

                var writer = JsonLineProtocol.writer(
                        socket.getOutputStream());

                JsonLineProtocol.write(
                        writer,
                        TransportFrame.unsubscribe(
                                "subscriber-A", "orders"));

                var response = JsonLineProtocol.read(reader);

                assertEquals("unsubscribed", response.kind());
                assertEquals(Boolean.TRUE, response.success());
            }

            // Abonamentul trebuie sa fie dezactivat in SQLite.
            try (var connection = store.openConnection();
                 var statement = connection.prepareStatement("""
                         SELECT active
                         FROM subscriptions
                         WHERE topic = ?
                           AND subscriber_id = ?
                         """)) {

                statement.setString(1, "orders");
                statement.setString(2, "subscriber-A");

                try (var result = statement.executeQuery()) {
                    assertTrue(result.next());
                    assertEquals(0, result.getInt("active"));
                }
            }

        } finally {
            server.close();
            executor.shutdownNow();
        }

    }

    // Verifica daca un Consumer dezabonat nu primeste livrari noi.
    @Test
    void unsubscribedConsumerDoesNotReceiveNewDeliveries()
            throws Exception {

        int port = 15104;

        var options = new BrokerOptions(port, 2, 50);

        var store = new PersistentMessageStore(
                tempDir.resolve("unsubscribe-delivery-test.db"));

        store.subscribe("orders", "subscriber-A");
        store.subscribe("orders", "subscriber-B");

        var registry = new ConsumerRegistry();
        var engine = new BrokerEngine(store, registry, options);
        var server = new BrokerServer(options, engine, registry);

        var executor = Executors.newSingleThreadExecutor();

        try {
            executor.submit(() -> {
                try {
                    server.run();
                } catch (Exception ignored) {
                }
            });

            Thread.sleep(150);

            // Dezabonam subscriber-A prin TCP.
            try (var socket = new java.net.Socket(
                    "127.0.0.1", port)) {

                socket.setSoTimeout(3000);

                var reader = JsonLineProtocol.reader(
                        socket.getInputStream());

                var writer = JsonLineProtocol.writer(
                        socket.getOutputStream());

                JsonLineProtocol.write(
                        writer,
                        TransportFrame.unsubscribe(
                                "subscriber-A", "orders"));

                var response = JsonLineProtocol.read(reader);

                assertEquals("unsubscribed", response.kind());
                assertEquals(Boolean.TRUE, response.success());
            }

            // Publicam un mesaj dupa dezabonare.
            var producer = new ProducerClient("127.0.0.1", port);

            var message = MessageFactory.create(
                    "Test",
                    "orders",
                    "{}",
                    "corr-after-unsubscribe"
            );

            assertTrue(producer.publish(message));

            // Verificam livrarile persistate.
            try (var connection = store.openConnection();
                 var statement = connection.prepareStatement("""
                         SELECT subscriber_id
                         FROM deliveries
                         WHERE message_id = ?
                         """)) {

                statement.setString(1, message.id());

                try (var result = statement.executeQuery()) {

                    assertTrue(result.next());

                    // Doar B trebuie sa aiba o livrare.
                    assertEquals(
                            "subscriber-B",
                            result.getString("subscriber_id"));

                    // Nu trebuie sa existe o a doua livrare.
                    assertFalse(result.next());
                }
            }

        } finally {
            server.close();
            executor.shutdownNow();
        }
    }

    // Verifica daca doi Consumer-i primesc si confirma acelasi mesaj.
    @Test
    void twoConsumersReceiveAndAcknowledgeSameMessage()
            throws Exception {

        int port = 15105;
        var options = new BrokerOptions(port, 3, 50);

        var store = new PersistentMessageStore(
                tempDir.resolve("two-consumers-test.db"));

        var registry = new ConsumerRegistry();
        var engine = new BrokerEngine(store, registry, options);
        var server = new BrokerServer(options, engine, registry);

        var executor = Executors.newSingleThreadExecutor();

        try {
            executor.submit(() -> {
                try {
                    server.run();
                } catch (Exception ignored) {
                }
            });

            Thread.sleep(150);

            // Deschidem doua conexiuni Consumer independente.
            try (var socketA = new java.net.Socket(
                    "127.0.0.1", port);
                 var socketB = new java.net.Socket(
                         "127.0.0.1", port)) {

                socketA.setSoTimeout(5000);
                socketB.setSoTimeout(5000);

                var readerA = JsonLineProtocol.reader(
                        socketA.getInputStream());
                var writerA = JsonLineProtocol.writer(
                        socketA.getOutputStream());

                var readerB = JsonLineProtocol.reader(
                        socketB.getInputStream());
                var writerB = JsonLineProtocol.writer(
                        socketB.getOutputStream());

                // Inregistram ambii Consumer-i.
                JsonLineProtocol.write(
                        writerA,
                        TransportFrame.register(
                                "subscriber-A", "orders"));

                assertEquals(
                        "registered",
                        JsonLineProtocol.read(readerA).kind());

                JsonLineProtocol.write(
                        writerB,
                        TransportFrame.register(
                                "subscriber-B", "orders"));

                assertEquals(
                        "registered",
                        JsonLineProtocol.read(readerB).kind());

                // Publicam un singur mesaj.
                var producer = new ProducerClient(
                        "127.0.0.1", port);

                var message = MessageFactory.create(
                        "OrderCreated",
                        "orders",
                        "{}",
                        "corr-two-consumers"
                );

                assertTrue(producer.publish(message));

                // Fiecare Consumer trebuie sa primeasca mesajul.
                var deliveryA = JsonLineProtocol.read(readerA);
                var deliveryB = JsonLineProtocol.read(readerB);

                assertEquals("delivery", deliveryA.kind());
                assertEquals("delivery", deliveryB.kind());

                assertEquals(
                        message.id(),
                        deliveryA.message().id());

                assertEquals(
                        message.id(),
                        deliveryB.message().id());

                // Fiecare confirma independent.
                JsonLineProtocol.write(
                        writerA,
                        TransportFrame.ack(message.id()));

                JsonLineProtocol.write(
                        writerB,
                        TransportFrame.ack(message.id()));

                // Asteptam confirmarea persistata de Broker.
                boolean bothAcknowledged = false;

                for (int i = 0; i < 50; i++) {

                    try (var connection = store.openConnection();
                         var statement = connection.prepareStatement("""
                                 SELECT COUNT(*)
                                 FROM deliveries
                                 WHERE message_id = ?
                                   AND status = 'ACKED'
                                 """)) {

                        statement.setString(1, message.id());

                        try (var result = statement.executeQuery()) {
                            assertTrue(result.next());

                            if (result.getInt(1) == 2) {
                                bothAcknowledged = true;
                                break;
                            }
                        }
                    }

                    Thread.sleep(50);
                }

                assertTrue(
                        bothAcknowledged,
                        "Both consumers must have persisted ACKs");
            }

        } finally {
            server.close();
            executor.shutdownNow();
        }
    }


}
