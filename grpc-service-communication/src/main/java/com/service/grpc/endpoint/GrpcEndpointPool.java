package com.service.grpc.endpoint;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import com.service.grpc.service.MessagingServiceGrpc;
import com.service.grpc.service.PublishReply;
import com.service.grpc.service.PublishRequest;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public final class GrpcEndpointPool implements AutoCloseable {

    private final List<ManagedChannel> channels = new ArrayList<>();
    private final AtomicInteger cursor = new AtomicInteger();

    public GrpcEndpointPool(String endpoints) {
        for (String endpoint : endpoints.split(",")) {
            String[] parts = endpoint.trim().split(":", 2);

            if (parts.length != 2) {
                throw new IllegalArgumentException(
                        "Endpoint must be host:port: " + endpoint
                );
            }

            channels.add(
                    ManagedChannelBuilder
                            .forAddress(parts[0], Integer.parseInt(parts[1]))
                            .usePlaintext()
                            .build()
            );
        }

        if (channels.isEmpty()) {
            throw new IllegalArgumentException(
                    "At least one gRPC endpoint is required"
            );
        }
    }

    public MessagingServiceGrpc.MessagingServiceBlockingStub nextBlockingStub() {
        ManagedChannel channel = channels.get(
                Math.floorMod(cursor.getAndIncrement(), channels.size())
        );

        return MessagingServiceGrpc.newBlockingStub(channel);
    }

    public MessagingServiceGrpc.MessagingServiceBlockingStub nextBlockingStub(
            long deadlineSeconds) {
        return nextBlockingStub().withDeadlineAfter(
                deadlineSeconds, TimeUnit.SECONDS
        );
    }

    public PublishReply publishWithFailover(PublishRequest request) {
        RuntimeException last = null;
        int start = Math.floorMod(cursor.getAndIncrement(), channels.size());

        for (int i = 0; i < channels.size(); i++) {
            ManagedChannel channel =
                    channels.get((start + i) % channels.size());

            try {
                return MessagingServiceGrpc.newBlockingStub(channel)
                        .withDeadlineAfter(5, TimeUnit.SECONDS)
                        .publish(request);
            } catch (RuntimeException e) {
                last = e;
            }
        }

        throw new IllegalStateException(
                "All configured gRPC broker endpoints failed; "
                        + "safe to retry with the same message_id",
                last
        );
    }

    @Override
    public void close() {
        for (ManagedChannel channel : channels) {
            channel.shutdown();
        }

        for (ManagedChannel channel : channels) {
            try {
                if (!channel.awaitTermination(2, TimeUnit.SECONDS)) {
                    channel.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                channel.shutdownNow();
            }
        }
    }
}