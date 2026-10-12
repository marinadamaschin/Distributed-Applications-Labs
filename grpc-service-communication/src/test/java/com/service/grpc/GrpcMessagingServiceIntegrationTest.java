package com.service.grpc;

import com.service.grpc.service.*;
import com.service.grpc.store.FileMessageStore;
import io.grpc.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class GrpcMessagingServiceIntegrationTest {

    @TempDir
    Path temp;
    private Server server;
    private ManagedChannel channel;
    private MessagingServiceGrpc.MessagingServiceBlockingStub stub;

    @BeforeEach
    void startServer() throws Exception {
        FileMessageStore store = new FileMessageStore(temp.resolve("journal.jsonl"));
        server = ServerBuilder.forPort(0).addService(new GrpcMessagingService(store)).build().start();
        channel = ManagedChannelBuilder.forAddress("127.0.0.1", server.getPort()).usePlaintext().build();
        stub = MessagingServiceGrpc.newBlockingStub(channel).withDeadlineAfter(3, TimeUnit.SECONDS);
    }

    @AfterEach
    void stopServer() throws Exception {
        if (channel != null) {
            channel.shutdownNow();
            channel.awaitTermination(2, TimeUnit.SECONDS);
        }

        if (server != null) {
            server.shutdownNow();
            server.awaitTermination(2, TimeUnit.SECONDS);
        }
    }

    private PublishRequest request(String id, String payload) {
        return PublishRequest.newBuilder().setMessageId(id).setTopic("orders").setPayload(payload)
                .setMessageType("OrderCreated").setCorrelationId("corr-1")
                .setOccurredAt("2026-10-11T10:00:00Z").setSchemaVersion("1").build();
    }

    @Test
    void publishAndStatusWorkOverGrpc() {
        assertTrue(stub.publish(request("m1", "{}")).getAccepted());
        StatusReply status = stub.getStatus(StatusRequest.newBuilder().build());
        assertEquals(1, status.getMessages());
    }

    @Test
    void invalidPublishIsRejectedWithInvalidArgument() {
        PublishRequest invalid = request("", "{}");
        StatusRuntimeException error = assertThrows(StatusRuntimeException.class, () -> stub.publish(invalid));
        assertEquals(io.grpc.Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
    }

    @Test
    void duplicateIdWithDifferentPayloadIsRejectedOverGrpc() {
        stub.publish(request("m1", "first"));
        StatusRuntimeException error = assertThrows(StatusRuntimeException.class, () -> stub.publish(request("m1", "second")));
        assertEquals(io.grpc.Status.Code.ALREADY_EXISTS, error.getStatus().getCode());
    }

    @Test
    void subscribeAckAndUnsubscribeUpdateStatus() {
        stub.publish(request("m1", "{}"));
        stub.subscribe(SubscribeRequest.newBuilder().setConsumerId("c1").setTopic("orders").build());
        assertTrue(stub.ack(AckRequest.newBuilder().setConsumerId("c1").setMessageId("m1").build()).getAccepted());
        assertTrue(stub.unsubscribe(UnsubscribeRequest.newBuilder().setConsumerId("c1").setTopic("orders").build()).getAccepted());

        StatusReply status = stub.getStatus(StatusRequest.newBuilder().build());
        assertEquals(1, status.getMessages());
        assertEquals(0, status.getSubscriptions());
    }

    @Test
    void ackOfUnknownMessageReturnsFalse() {
        assertFalse(stub.ack(AckRequest.newBuilder().setConsumerId("c1").setMessageId("missing").build()).getAccepted());
    }
}