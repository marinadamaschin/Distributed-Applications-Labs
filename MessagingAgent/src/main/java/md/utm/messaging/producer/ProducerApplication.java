package md.utm.messaging.producer;

import md.utm.messaging.orders.OrderMessageBuilder;

import java.util.UUID;

public final class ProducerApplication {
    public static void main(String[] args) throws Exception {
        String host = System.getenv().getOrDefault("BROKER_HOST", "127.0.0.1");
        int port = Integer.parseInt(System.getenv().getOrDefault("BROKER_PORT", "5000"));
        String correlationId = UUID.randomUUID().toString().replace("-", "");
        var client = new ProducerClient(host, port);
        client.publish(OrderMessageBuilder.order("{\"orderId\":\"ORD-1001\",\"amount\":42.50}", correlationId));
        client.publish(OrderMessageBuilder.audit("{\"action\":\"order-created\",\"orderId\":\"ORD-1001\"}", correlationId));
    }
}
