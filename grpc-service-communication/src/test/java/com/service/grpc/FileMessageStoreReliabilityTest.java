package com.service.grpc;

import com.service.grpc.service.PublishRequest;
import com.service.grpc.store.FileMessageStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class FileMessageStoreReliabilityTest {

    @TempDir
    Path temp;

    private PublishRequest request(String id, String topic, String payload) {
        return PublishRequest.newBuilder().setMessageId(id).setTopic(topic).setPayload(payload)
                .setMessageType("OrderCreated").setCorrelationId("corr-" + id)
                .setOccurredAt("2026-10-11T10:00:00Z").setSchemaVersion("1").build();
    }

    @Test
    void subscriptionAndUnsubscriptionSurviveRestart() throws Exception {
        Path journal = temp.resolve("messages.jsonl");
        FileMessageStore store = new FileMessageStore(journal);

        store.subscribe("consumer-a", "orders");
        assertEquals(1, store.subscriptionCount());

        FileMessageStore restarted = new FileMessageStore(journal);
        assertEquals(1, restarted.subscriptionCount());

        restarted.unsubscribe("consumer-a", "orders");
        assertEquals(0, new FileMessageStore(journal).subscriptionCount());
    }

    @Test
    void backupCanRestorePreviousJournalState() throws Exception {
        Path journal = temp.resolve("messages.jsonl");
        FileMessageStore store = new FileMessageStore(journal);
        store.publish(request("m1", "orders", "one"));

        Path backup = store.backup(temp.resolve("backups"));
        store.publish(request("m2", "orders", "two"));
        assertEquals(2, store.messageCount());

        store.restore(backup);
        assertEquals(1, new FileMessageStore(journal).messageCount());
        assertTrue(Files.size(backup) > 0);
    }

    @Test
    void unknownAckIsRejectedAndKnownAckIsIdempotent() throws Exception {
        FileMessageStore store = new FileMessageStore(temp.resolve("messages.jsonl"));
        assertFalse(store.ack("consumer-a", "missing"));
        store.publish(request("m1", "orders", "one"));
        store.subscribe("consumer-a", "orders");

        assertTrue(store.ack("consumer-a", "m1"));
        assertTrue(store.ack("consumer-a", "m1"));
        assertTrue(new FileMessageStore(temp.resolve("messages.jsonl")).pending("consumer-a", "orders").isEmpty());
    }

    @Test
    void messagesAreIsolatedByTopicAndConsumerAck() throws Exception {
        FileMessageStore store = new FileMessageStore(temp.resolve("messages.jsonl"));
        store.publish(request("m1", "orders", "one"));
        store.publish(request("m2", "payments", "two"));
        store.subscribe("consumer-a", "orders");
        store.subscribe("consumer-b", "orders");
        assertEquals(1, store.pending("consumer-a", "orders").size());
        assertTrue(store.pending("consumer-a", "payments").isEmpty());

        store.ack("consumer-a", "m1");
        assertTrue(store.pending("consumer-a", "orders").isEmpty());
        assertEquals(1, store.pending("consumer-b", "orders").size());
    }

    @Test
    void deliveryAckAndNackRequireConsumerTopicSubscription() throws Exception {
        FileMessageStore store = new FileMessageStore(temp.resolve("messages.jsonl"));
        store.publish(request("m1", "orders", "one"));

        assertTrue(store.pending("consumer-a", "orders").isEmpty());
        assertFalse(store.ack("consumer-a", "m1"));
        assertFalse(store.nack("consumer-a", "m1", "not subscribed"));

        store.subscribe("consumer-a", "orders");
        assertEquals(1, store.pending("consumer-a", "orders").size());
        store.unsubscribe("consumer-a", "orders");

        assertTrue(store.pending("consumer-a", "orders").isEmpty());
        assertFalse(store.ack("consumer-a", "m1"));
        assertFalse(store.nack("consumer-a", "m1", "subscription removed"));
    }

    @Test
    void duplicateIdWithChangedTopicOrMetadataIsRejected() throws Exception {
        FileMessageStore store = new FileMessageStore(temp.resolve("messages.jsonl"));
        store.publish(request("m1", "orders", "one"));
        assertThrows(FileMessageStore.DuplicateMessageException.class,
                () -> store.publish(request("m1", "payments", "one")));
    }

    @Test
    void journalContainsAppendOnlyJsonEvents() throws Exception {
        Path journal = temp.resolve("messages.jsonl");
        FileMessageStore store = new FileMessageStore(journal);
        store.publish(request("m1", "orders", "one"));
        store.subscribe("consumer-a", "orders");
        store.ack("consumer-a", "m1");
        String content = Files.readString(journal);
        assertTrue(content.contains("\"event\":\"publish\""));
        assertTrue(content.contains("\"event\":\"subscribe\""));
        assertTrue(content.contains("\"event\":\"ack\""));
        assertEquals(3, content.lines().count());
    }

    @Test
    void missingBackupFailsClearly() throws Exception {
        FileMessageStore store = new FileMessageStore(temp.resolve("messages.jsonl"));
        assertThrows(java.io.IOException.class, () -> store.restore(temp.resolve("does-not-exist.jsonl")));
    }
}