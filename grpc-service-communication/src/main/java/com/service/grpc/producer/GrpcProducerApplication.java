package com.service.grpc.producer;

import com.service.grpc.endpoint.GrpcEndpointPool;
import com.service.grpc.service.*;

import java.time.OffsetDateTime;
import java.util.UUID;

public final class GrpcProducerApplication {

    public static void main(String[] args) throws Exception {
        String endpoints = System.getenv().getOrDefault("GRPC_ENDPOINTS", System.getenv().getOrDefault("GRPC_HOST", "127.0.0.1") + ":" + System.getenv().getOrDefault("GRPC_PORT", "50051"));

        try (GrpcEndpointPool pool = new GrpcEndpointPool(endpoints)) {
            String correlation = UUID.randomUUID().toString();

            for (String topic : new String[]{"orders", "audit"}) {
                PublishRequest request = PublishRequest.newBuilder().setMessageId(UUID.randomUUID().toString())
                        .setTopic(topic).setPayload("{\"orderId\":\"ORD-1001\",\"amount\":42.50}")
                        .setMessageType(topic.equals("orders") ? "OrderCreated" : "AuditEvent")
                        .setCorrelationId(correlation).setOccurredAt(OffsetDateTime.now().toString()).setSchemaVersion("1.0").build();

                PublishReply reply = pool.publishWithFailover(request);

                if (!reply.getAccepted()) {
                    throw new IllegalStateException("Broker rejected message: " + reply.getDetail());
                }

                System.out.println("Published " + request.getMessageId() + " -> " + topic);
            }
        }
    }
}