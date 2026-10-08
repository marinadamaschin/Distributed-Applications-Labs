package md.utm.messaging.producer;

import md.utm.messaging.contracts.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;

public final class ProducerClient {

    private static final int CONNECT_TIMEOUT_MS = 2000;
    private static final int RESPONSE_TIMEOUT_MS = 3000;

    private final String host;
    private final int port;

    public ProducerClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public boolean publish(MessageEnvelope message) throws IOException {
        try (Socket socket = new Socket()) {

            socket.connect(
                    new InetSocketAddress(host, port),
                    CONNECT_TIMEOUT_MS
            );

            socket.setSoTimeout(RESPONSE_TIMEOUT_MS);

            var reader = JsonLineProtocol.reader(
                    socket.getInputStream()
            );

            var writer = JsonLineProtocol.writer(
                    socket.getOutputStream()
            );

            JsonLineProtocol.write(
                    writer,
                    TransportFrame.publish(message)
            );

            TransportFrame response =
                    JsonLineProtocol.read(reader);

            boolean accepted =
                    "publishAccepted".equals(response.kind())
                            && Boolean.TRUE.equals(response.success())
                            && message.id().equals(response.messageId());

            StructuredLog.write(
                    accepted ? "INFO" : "ERROR",
                    "producer",
                    accepted ? "BROKER_ACCEPTED" : "BROKER_REJECTED",
                    message,
                    accepted ? "accepted" : "rejected",
                    null
            );

            return accepted;

        } catch (SocketTimeoutException e) {
            throw new IOException(
                    "Timeout waiting for Broker confirmation", e
            );
        }
    }
}
