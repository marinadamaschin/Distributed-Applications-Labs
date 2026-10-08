package md.utm.messaging.broker;

import md.utm.messaging.contracts.JsonLineProtocol;
import md.utm.messaging.contracts.TransportFrame;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public final class BrokerServer implements AutoCloseable {

    private final BrokerOptions options;
    private final BrokerEngine engine;
    private final ConsumerRegistry registry;

    private final ExecutorService clients =
            Executors.newCachedThreadPool();

    private volatile boolean running = true;
    private ServerSocket server;

    public BrokerServer(
            BrokerOptions options,
            BrokerEngine engine,
            ConsumerRegistry registry) {

        this.options = options;
        this.engine = engine;
        this.registry = registry;
    }

    public void run() throws IOException {
        server = new ServerSocket(options.port());

        System.out.println(
                "Broker listening on port " + options.port());

        while (running) {
            try {
                Socket socket = server.accept();
                clients.submit(() -> handle(socket));

            } catch (SocketException e) {
                if (running) {
                    throw e;
                }
            }
        }
    }

    private void handle(Socket socket) {
        boolean ownedBySubscriber = false;

        try {
            var reader =
                    JsonLineProtocol.reader(socket.getInputStream());

            var writer =
                    JsonLineProtocol.writer(socket.getOutputStream());

            TransportFrame frame = JsonLineProtocol.read(reader);

            if ("publish".equals(frame.kind())) {

                if (frame.message() == null) {
                    JsonLineProtocol.write(
                            writer,
                            TransportFrame.error(
                                    "Missing message"));
                    return;
                }

                try {
                    boolean accepted =
                            engine.publish(frame.message());

                    JsonLineProtocol.write(
                            writer,
                            accepted
                                    ? TransportFrame.accepted(
                                    frame.message().id())
                                    : TransportFrame.error(
                                    "Invalid message or schemaVersion"));

                } catch (Exception e) {
                    System.err.println(
                            "Publish failed: " + e.getMessage());

                    JsonLineProtocol.write(
                            writer,
                            TransportFrame.error(
                                    "Message could not be persisted"));
                }

            } else if ("registerConsumer".equals(frame.kind())) {

                String topic = frame.destination();
                String subscriberId = frame.consumerId();

                if (topic == null || topic.isBlank()
                        || subscriberId == null
                        || subscriberId.isBlank()) {

                    JsonLineProtocol.write(
                            writer,
                            TransportFrame.error(
                                    "Invalid subscriber registration"));
                    return;
                }

                // Abonamentul devine persistent.
                engine.subscribe(topic, subscriberId);

                ConsumerConnection connection =
                        new ConsumerConnection(
                                subscriberId,
                                topic,
                                socket,
                                reader,
                                writer);

                // Confirmam inregistrarea inainte de livrari.
                JsonLineProtocol.write(
                        writer,
                        TransportFrame.registered(
                                subscriberId, topic));

                registry.register(topic, connection);
                ownedBySubscriber = true;

                engine.subscriberConnected(
                        topic, subscriberId);

                System.out.println(
                        "Consumer registered: "
                                + subscriberId + " -> " + topic);

            } else if ("unsubscribe".equals(frame.kind())) {

                String topic = frame.destination();
                String subscriberId = frame.consumerId();

                if (topic == null || topic.isBlank()
                        || subscriberId == null
                        || subscriberId.isBlank()) {

                    JsonLineProtocol.write(
                            writer,
                            TransportFrame.error(
                                    "Invalid unsubscribe request"));
                    return;
                }

                // Dezactiveaza abonamentul in SQLite
                // si anuleaza livrarile nefinalizate.
                engine.unsubscribe(topic, subscriberId);

                // Confirma dezabonarea.
                JsonLineProtocol.write(
                        writer,
                        TransportFrame.unsubscribed(
                                subscriberId, topic));

                // Elimina si inchide conexiunea activa.
                registry.disconnect(topic, subscriberId);

                System.out.println(
                        "Consumer unsubscribed: "
                                + subscriberId + " -> " + topic);

            } else {

                JsonLineProtocol.write(
                        writer,
                        TransportFrame.error(
                                "Unsupported frame kind"));
            }

        } catch (Exception e) {
            System.err.println(
                    "Client error: " + e.getMessage());

        } finally {
            if (!ownedBySubscriber) {
                try {
                    socket.close();
                } catch (IOException ignored) {
                }
            }
        }
    }

    @Override
    public void close() {
        running = false;

        try {
            if (server != null) {
                server.close();
            }
        } catch (IOException ignored) {
        }

        clients.shutdownNow();
        engine.shutdown();
        registry.closeAll();
    }
}
