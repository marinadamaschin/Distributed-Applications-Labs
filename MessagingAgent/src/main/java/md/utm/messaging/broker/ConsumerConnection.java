package md.utm.messaging.broker;

import md.utm.messaging.contracts.JsonLineProtocol;
import md.utm.messaging.contracts.MessageEnvelope;
import md.utm.messaging.contracts.TransportFrame;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.net.Socket;

public final class ConsumerConnection implements AutoCloseable {

    private static final int ACK_TIMEOUT_MS = 5000;

    private final String consumerId;
    private final String destination;
    private final Socket socket;
    private final BufferedReader reader;
    private final BufferedWriter writer;

    public ConsumerConnection(
            String id,
            String dest,
            Socket socket,
            BufferedReader reader,
            BufferedWriter writer) throws IOException {

        this.consumerId = id;
        this.destination = dest;
        this.socket = socket;
        this.reader = reader;
        this.writer = writer;

        // Limita de asteptare pentru ACK/NACK.
        this.socket.setSoTimeout(ACK_TIMEOUT_MS);
    }

    public String consumerId() {
        return consumerId;
    }

    public String destination() {
        return destination;
    }

    public synchronized boolean deliver(
            MessageEnvelope message,
            int attempt) throws IOException {

        JsonLineProtocol.write(
                writer,
                TransportFrame.delivery(message, attempt));

        TransportFrame response = JsonLineProtocol.read(reader);

        if (response == null) {
            throw new IOException(
                    "Subscriber disconnected before ACK");
        }

        if ("ack".equals(response.kind())
                && message.id().equals(response.messageId())) {
            return true;
        }

        if ("nack".equals(response.kind())
                && message.id().equals(response.messageId())) {
            return false;
        }

        throw new IOException(
                "Invalid ACK/NACK for message " + message.id());
    }

    @Override
    public void close() throws IOException {
        socket.close();
    }
}
