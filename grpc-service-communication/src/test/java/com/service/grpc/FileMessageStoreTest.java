package com.service.grpc;

import com.service.grpc.service.PublishRequest;
import com.service.grpc.store.FileMessageStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class FileMessageStoreTest {

    @TempDir
    Path temp;

    private PublishRequest request(String id, String payload) {
        return PublishRequest.newBuilder().setMessageId(id).setTopic("orders").setPayload(payload)
                .setMessageType("OrderCreated").setCorrelationId("c-1").setOccurredAt("2026-10-11T10:00:00Z").setSchemaVersion("1").build();
    }

    @Test
    void publishAndAckSurviveRestart() throws Exception {
        Path journal = temp.resolve("messages.jsonl");
        FileMessageStore a = new FileMessageStore(journal);
        a.publish(request("m1", "{}"));
        a.subscribe("c1", "orders");
        assertEquals(1, a.pending("c1", "orders").size());
        assertTrue(a.ack("c1", "m1"));

        FileMessageStore b = new FileMessageStore(journal);
        assertTrue(b.pending("c1", "orders").isEmpty());
        assertEquals(1, b.messageCount());
    }

    @Test
    void identicalPublishIsIdempotentButDifferentContentIsRejected() throws Exception {
        FileMessageStore s = new FileMessageStore(temp.resolve("messages.jsonl"));
        s.publish(request("m1", "{}"));
        assertDoesNotThrow(() -> s.publish(request("m1", "{}")));
        assertThrows(FileMessageStore.DuplicateMessageException.class, () -> s.publish(request("m1", "different")));
    }

    @Test
    void nackAndRetryLimitPersistDeadLetter() throws Exception {
        FileMessageStore s = new FileMessageStore(temp.resolve("messages.jsonl"));
        s.publish(request("m1", "{}"));
        s.subscribe("c1", "orders");

        var first = s.pending("c1", "orders");
        assertFalse(first.isEmpty());
        s.nack("c1", "m1", "test failure");
        assertTrue(s.pending("c1", "orders").isEmpty(), "NACK must respect retry backoff");

        Thread.sleep(1_100);
        var second = s.pending("c1", "orders");
        assertFalse(second.isEmpty());
        s.nack("c1", "m1", "test failure");

        assertTrue(s.pending("c1", "orders").isEmpty(), "second retry must also be delayed");
        Thread.sleep(2_100);
        var third = s.pending("c1", "orders");
        assertFalse(third.isEmpty());
        s.nack("c1", "m1", "test failure");

        assertEquals(1, s.deadLetterCount());

        FileMessageStore restarted = new FileMessageStore(temp.resolve("messages.jsonl"));
        assertEquals(1, restarted.deadLetterCount());
    }
}