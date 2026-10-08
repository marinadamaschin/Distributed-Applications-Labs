package md.utm.messaging.consumers;

import md.utm.messaging.contracts.*;

import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.time.*;

public final class ConsumerClient {
    private final String host, consumerId, destination;
    private final int port;
    private final ConsumerMode mode;
    private final DeduplicationStore dedup;
    private final Path effects;
    private volatile boolean unsubscribeRequested = false;

    public ConsumerClient(String h, int p, String id, String d, ConsumerMode m, Path dataDir) throws IOException {
        host = h;
        port = p;
        consumerId = id;
        destination = d;
        mode = m;
        dedup = new DeduplicationStore(dataDir.resolve("processed-" + id + ".txt"));
        effects = dataDir.resolve("effects-" + id + ".log");
    }

    public void run() throws IOException {

        final long reconnectDelayMs = 2000;

        while (!Thread.currentThread().isInterrupted()
                && !unsubscribeRequested) {

            try (Socket socket = new Socket(host, port);
                 var reader = JsonLineProtocol.reader(
                         socket.getInputStream());
                 var writer = JsonLineProtocol.writer(
                         socket.getOutputStream())) {

                JsonLineProtocol.write(
                        writer,
                        TransportFrame.register(
                                consumerId, destination));

                TransportFrame registration =
                        JsonLineProtocol.read(reader);

                if (!"registered".equals(registration.kind())) {
                    throw new IOException(
                            "Registration rejected: "
                                    + registration.kind());
                }

                System.out.println(
                        "Registered " + consumerId
                                + " -> " + destination
                                + " mode=" + mode);

                while (!Thread.currentThread().isInterrupted()
                        && !unsubscribeRequested) {

                    TransportFrame frame =
                            JsonLineProtocol.read(reader);

                    if (!"delivery".equals(frame.kind())
                            || frame.message() == null) {
                        continue;
                    }

                    process(frame, writer);
                }

            } catch (IOException e) {

                if (Thread.currentThread().isInterrupted()
                        || unsubscribeRequested) {
                    break;
                }

                System.err.println(
                        "[RECONNECT] Consumer " + consumerId
                                + " disconnected: "
                                + e.getMessage());

            }

            if (Thread.currentThread().isInterrupted()
                    || unsubscribeRequested) {
                break;
            }

            try {
                Thread.sleep(reconnectDelayMs);

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        System.out.println(
                "Consumer stopped: " + consumerId);
    }

    private void process(TransportFrame f, BufferedWriter w) throws IOException {
        MessageEnvelope m = f.message();
        StructuredLog.write("INFO", "consumer", "RECEIVED", m, null, "consumer=" + consumerId + " attempt=" + f.attempt());
        if (mode == ConsumerMode.NO_ACK) {
            StructuredLog.write(
                    "WARN",
                    "consumer",
                    "NO_ACK_SIMULATION",
                    m,
                    "simulated-hang",
                    "consumer=" + consumerId
            );

            try {
                Thread.sleep(15_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(
                        "NO_ACK simulation interrupted", e);
            }

            return;
        }
        if (dedup.contains(m.id())) {
            StructuredLog.write("INFO", "consumer", "DUPLICATE", m, "effect-skipped", "consumer=" + consumerId);
            JsonLineProtocol.write(w, TransportFrame.ack(m.id()));
            StructuredLog.write("INFO", "consumer", "ACK_SENT", m, "success", null);
            return;
        }
        if (mode == ConsumerMode.ALWAYS_NACK) {
            StructuredLog.write("WARN", "consumer", "PROCESSING_FAILED", m, "simulated", null);
            JsonLineProtocol.write(w, TransportFrame.nack(m.id(), "Simulated failure"));
            StructuredLog.write("WARN", "consumer", "NACK_SENT", m, "retry", null);
            return;
        }
        try {
            Files.writeString(effects, OffsetDateTime.now(ZoneOffset.UTC) + " | messageId=" + m.id() + " | correlationId=" + m.correlationId() + " | type=" + m.type() + " | payload=" + m.payload() + System.lineSeparator(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            dedup.markProcessed(m.id());
            StructuredLog.write("INFO", "consumer", "PROCESSED", m, "success", null);
            if (mode == ConsumerMode.CRASH_BEFORE_ACK) {
                StructuredLog.write("ERROR", "consumer", "CRASH_BEFORE_ACK", m, "intentional-exit", null);
                System.exit(17);
            }
            JsonLineProtocol.write(w, TransportFrame.ack(m.id()));
            StructuredLog.write("INFO", "consumer", "ACK_SENT", m, "success", null);
        } catch (IOException e) {
            StructuredLog.write("ERROR", "consumer", "PROCESSING_ERROR", m, "failed", "reason=" + e.getMessage());
            JsonLineProtocol.write(w, TransportFrame.nack(m.id(), e.getMessage()));
        }
    }


    public boolean unsubscribe() throws IOException {

        try (Socket socket = new Socket(host, port);
             var reader = JsonLineProtocol.reader(
                     socket.getInputStream());
             var writer = JsonLineProtocol.writer(
                     socket.getOutputStream())) {

            socket.setSoTimeout(3000);

            JsonLineProtocol.write(
                    writer,
                    TransportFrame.unsubscribe(
                            consumerId, destination)
            );

            TransportFrame response =
                    JsonLineProtocol.read(reader);

            boolean confirmed =
                    "unsubscribed".equals(response.kind())
                            && Boolean.TRUE.equals(response.success())
                            && consumerId.equals(response.consumerId())
                            && destination.equals(response.destination());

            if (confirmed) {
                unsubscribeRequested = true;
                System.out.println(
                        "Unsubscribed: " + consumerId
                                + " -> " + destination);
            }

            return confirmed;
        }
    }


}
