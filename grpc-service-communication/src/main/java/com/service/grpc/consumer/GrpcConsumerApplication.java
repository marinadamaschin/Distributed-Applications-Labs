package com.service.grpc.consumer;

import com.service.grpc.endpoint.GrpcEndpointPool;
import com.service.grpc.service.*;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.util.Iterator;

public final class GrpcConsumerApplication {

    private static final long INITIAL_BACKOFF_MS = 500;
    private static final long MAX_BACKOFF_MS = 10_000;

    public static void main(String[] args) throws InterruptedException {
        if (args.length < 2) {
            System.out.println(
                    "Usage: GrpcConsumerApplication <consumerId> <topic> "
                            + "[normal|always-nack|crash-before-ack]"
            );
            return;
        }

        String consumerId = args[0];
        String topic = args[1];
        String mode = args.length >= 3 ? args[2] : "normal";

        String endpoints = System.getenv().getOrDefault(
                "GRPC_ENDPOINTS",
                System.getenv().getOrDefault("GRPC_HOST", "127.0.0.1")
                        + ":"
                        + System.getenv().getOrDefault("GRPC_PORT", "50051")
        );

        SubscribeRequest request = SubscribeRequest.newBuilder()
                .setConsumerId(consumerId)
                .setTopic(topic)
                .build();

        try (GrpcEndpointPool pool = new GrpcEndpointPool(endpoints)) {
            long backoff = INITIAL_BACKOFF_MS;

            while (!Thread.currentThread().isInterrupted()) {
                System.out.println(
                        "[CONSUMER] Connecting to a configured broker endpoint..."
                );

                try {
                    MessagingServiceGrpc.MessagingServiceBlockingStub streamClient =
                            pool.nextBlockingStub();

                    Iterator<Delivery> deliveries =
                            streamClient.subscribe(request);

                    while (deliveries.hasNext()) {
                        Delivery delivery = deliveries.next();

                        backoff = INITIAL_BACKOFF_MS;

                        processDelivery(pool, consumerId, mode, delivery);
                    }

                    throw new IllegalStateException(
                            "Subscribe stream completed unexpectedly"
                    );

                } catch (StatusRuntimeException e) {
                    Status.Code code = e.getStatus().getCode();

                    if (code != Status.Code.UNAVAILABLE
                            && code != Status.Code.DEADLINE_EXCEEDED
                            && code != Status.Code.CANCELLED) {
                        throw e;
                    }

                    System.err.println(
                            "[CONSUMER] gRPC stream failed: "
                                    + e.getStatus()
                                    + ". Retry in " + backoff + " ms"
                    );

                } catch (IllegalStateException e) {
                    System.err.println(
                            "[CONSUMER] " + e.getMessage()
                                    + ". Retry in " + backoff + " ms"
                    );
                }

                try {
                    Thread.sleep(backoff);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }

                backoff = Math.min(backoff * 2, MAX_BACKOFF_MS);
            }
        }
    }

    private static void processDelivery(
            GrpcEndpointPool pool,
            String consumerId,
            String mode,
            Delivery delivery) {

        System.out.printf(
                "Received id=%s topic=%s type=%s payload=%s attempt=%d%n",
                delivery.getMessageId(),
                delivery.getTopic(),
                delivery.getMessageType(),
                delivery.getPayload(),
                delivery.getAttempt()
        );

        if (mode.equalsIgnoreCase("crash-before-ack")) {
            System.err.println(
                    "[FAILURE-INJECTION] Simulating consumer crash before ACK"
            );
            System.exit(2);
        }

        try {
            MessagingServiceGrpc.MessagingServiceBlockingStub rpc =
                    pool.nextBlockingStub(3);

            if (mode.equalsIgnoreCase("always-nack")) {
                AckReply reply = rpc.nack(
                        NackRequest.newBuilder()
                                .setConsumerId(consumerId)
                                .setMessageId(delivery.getMessageId())
                                .setReason("failure injection: always-nack")
                                .build()
                );

                System.out.println(
                        "NACK sent: " + reply.getAccepted()
                                + " for " + delivery.getMessageId()
                );
            } else {
                AckReply reply = rpc.ack(
                        AckRequest.newBuilder()
                                .setConsumerId(consumerId)
                                .setMessageId(delivery.getMessageId())
                                .build()
                );

                if (reply.getAccepted()) {
                    System.out.println(
                            "ACK persisted: " + delivery.getMessageId()
                    );
                } else {
                    System.err.println(
                            "ACK rejected for " + delivery.getMessageId()
                    );
                }
            }
        } catch (RuntimeException e) {
            System.err.println(
                    "ACK/NACK failed; message may be redelivered: "
                            + e.getMessage()
            );
        }
    }
}