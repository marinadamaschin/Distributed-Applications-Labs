package md.utm.messaging.producer;

import md.utm.messaging.contracts.*;

import java.io.*;
import java.net.*;

public final class ProducerClient {
    private final String host;
    private final int port;

    public ProducerClient(String h, int p) {
        host = h;
        port = p;
    }

    public boolean publish(MessageEnvelope m) throws IOException {
        try (Socket s = new Socket(host, port); var r = JsonLineProtocol.reader(s.getInputStream()); var w = JsonLineProtocol.writer(s.getOutputStream())) {
            JsonLineProtocol.write(w, TransportFrame.publish(m));
            TransportFrame response = JsonLineProtocol.read(r);
            boolean ok = "publishAccepted".equals(response.kind()) && Boolean.TRUE.equals(response.success());
            StructuredLog.write(ok ? "INFO" : "ERROR", "producer", ok ? "BROKER_ACCEPTED" : "BROKER_REJECTED", m, ok ? "accepted" : "rejected", null);
            return ok;
        }
    }
}
