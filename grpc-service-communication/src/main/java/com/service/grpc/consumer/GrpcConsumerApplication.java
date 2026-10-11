package com.service.grpc.consumer;

import com.service.grpc.endpoint.GrpcEndpointPool;
import com.service.grpc.service.*;

public final class GrpcConsumerApplication {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Usage: GrpcConsumerApplication <consumerId> <topic> [normal|always-nack|crash-before-ack]");
            return;
        }

        String mode = args.length >= 3 ? args[2] : "normal";
        String endpoints = System.getenv().getOrDefault("GRPC_ENDPOINTS", System.getenv().getOrDefault("GRPC_HOST", "127.0.0.1") + ":" + System.getenv().getOrDefault("GRPC_PORT", "50051"));

        try (GrpcEndpointPool pool = new GrpcEndpointPool(endpoints)) {
            MessagingServiceGrpc.MessagingServiceBlockingStub client = pool.nextBlockingStub(10);
            SubscribeRequest request = SubscribeRequest.newBuilder().setConsumerId(args[0]).setTopic(args[1]).build();

            client.subscribe(request).forEachRemaining(delivery -> {
                System.out.printf("Received id=%s topic=%s type=%s payload=%s%n", delivery.getMessageId(), delivery.getTopic(), delivery.getMessageType(), delivery.getPayload());

                if (mode.equalsIgnoreCase("crash-before-ack")) {
                    System.err.println("[FAILURE-INJECTION] Simulating consumer crash before ACK");
                    System.exit(2);
                }

                try {
                    if (mode.equalsIgnoreCase("always-nack")) {
                        AckReply nack = client.nack(NackRequest.newBuilder().setConsumerId(args[0]).setMessageId(delivery.getMessageId()).setReason("failure injection: always-nack").build());
                        System.out.println("NACK sent: " + nack.getAccepted() + " for " + delivery.getMessageId());
                    } else {
                        AckReply ack = client.ack(AckRequest.newBuilder().setConsumerId(args[0]).setMessageId(delivery.getMessageId()).build());
                        if (ack.getAccepted()) System.out.println("ACK persisted: " + delivery.getMessageId());
                    }
                } catch (Exception e) {
                    System.err.println("ACK/NACK failed; message may be redelivered: " + e.getMessage());
                }
            });
        }
    }
}