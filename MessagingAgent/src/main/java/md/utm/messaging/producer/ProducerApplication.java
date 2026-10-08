package md.utm.messaging.producer;

import md.utm.messaging.orders.OrderMessageBuilder;

import java.io.IOException;
import java.util.UUID;

public final class ProducerApplication {

    public static void main(String[] args) {

        String host = System.getenv()
                .getOrDefault("BROKER_HOST", "127.0.0.1");

        int port = Integer.parseInt(
                System.getenv().getOrDefault("BROKER_PORT", "5000")
        );

        String correlationId = UUID.randomUUID()
                .toString()
                .replace("-", "");

        ProducerClient client = new ProducerClient(host, port);
        ReliableProducer producer = new ReliableProducer(client, 3, 1000);

        var order = OrderMessageBuilder.order(
                "{\"orderId\":\"ORD-1001\",\"amount\":42.50}",
                correlationId
        );

        var audit = OrderMessageBuilder.audit(
                "{\"action\":\"order-created\",\"orderId\":\"ORD-1001\"}",
                correlationId
        );

        try {
            if (!producer.publish(order)) {
                System.err.println(
                        "[PRODUCER] Order rejected by Broker."
                );
                System.exit(1);
            }

            if (!producer.publish(audit)) {
                System.err.println(
                        "[PRODUCER] Audit rejected by Broker."
                );
                System.exit(1);
            }

            System.out.println(
                    "[PRODUCER] All messages published successfully."
            );

        } catch (IOException e) {
            System.err.println(
                    "[PRODUCER ERROR] Broker unavailable or confirmation failed."
            );
            System.err.println(
                    "[PRODUCER ERROR] Details: " + e.getMessage()
            );
            System.exit(1);
        }
    }
}
