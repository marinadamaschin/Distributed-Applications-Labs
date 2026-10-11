package com.service.grpc.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.service.grpc.service.Delivery;
import com.service.grpc.service.PublishRequest;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;

public final class FileMessageStore {

    private static final int MAX_ATTEMPTS = 3;
    private static final long RETRY_BASE_MS = 1_000;

    private final Path journal;
    private final Path lockPath;
    private final ObjectMapper mapper = new ObjectMapper();

    private final Map<String, StoredMessage> messages = new LinkedHashMap<>();
    private final Set<String> acknowledgements = new HashSet<>();
    private final Map<String, Integer> attempts = new HashMap<>();
    private final Map<String, Long> lastDelivery = new HashMap<>();
    private final Set<String> deadLetters = new HashSet<>();
    private final Set<String> subscriptions = new HashSet<>();

    public FileMessageStore(Path journal) throws IOException {
        this.journal = journal.toAbsolutePath().normalize();
        this.lockPath = this.journal.resolveSibling(this.journal.getFileName() + ".lock");

        Path parent = this.journal.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }

        if (!Files.exists(this.journal)) {
            try {
                Files.createFile(this.journal);
            } catch (FileAlreadyExistsException ignored) {
            }
        }

        synchronized (this) {
            reload();
        }
    }

    public synchronized boolean publish(PublishRequest request) throws IOException {
        withFileLock(() -> {
            reload();

            StoredMessage candidate = new StoredMessage(
                    request.getMessageId(), request.getTopic(), request.getPayload(),
                    request.getMessageType(), request.getCorrelationId(), request.getOccurredAt(), request.getSchemaVersion()
            );
            StoredMessage previous = messages.get(candidate.messageId());

            if (previous != null) {
                if (!previous.equals(candidate)) {
                    throw new DuplicateMessageException("message_id already exists with different content");
                }
                return;
            }

            appendUnlocked(Map.of("event", "publish", "message", candidate));
            messages.put(candidate.messageId(), candidate);
        });

        return true;
    }

    public synchronized void subscribe(String consumer, String topic) throws IOException {
        String key = key(consumer, topic);

        withFileLock(() -> {
            reload();
            if (subscriptions.add(key)) {
                appendUnlocked(Map.of("event", "subscribe", "consumerId", consumer, "topic", topic));
            }
        });
    }

    public synchronized void unsubscribe(String consumer, String topic) throws IOException {
        String key = key(consumer, topic);

        withFileLock(() -> {
            reload();
            if (subscriptions.remove(key)) {
                appendUnlocked(Map.of("event", "unsubscribe", "consumerId", consumer, "topic", topic));
            }
        });
    }

    public synchronized List<DeliveryCandidate> pending(String consumer, String topic) throws IOException {
        List<DeliveryCandidate> result = new ArrayList<>();

        withFileLock(() -> {
            reload();
            long now = System.currentTimeMillis();

            for (StoredMessage m : messages.values()) {
                String k = key(consumer, m.messageId());

                if (!m.topic().equals(topic) || acknowledgements.contains(k) || deadLetters.contains(k)) {
                    continue;
                }

                int count = attempts.getOrDefault(k, 0);
                long previous = lastDelivery.getOrDefault(k, 0L);
                long delay = count == 0 ? 0 : RETRY_BASE_MS * (1L << Math.min(count - 1, 5));

                if (count >= MAX_ATTEMPTS) {
                    if (now - previous >= delay) {
                        deadLetterUnlocked(consumer, m.messageId(), "maximum attempts reached");
                    }
                    continue;
                }

                if (now - previous < delay) {
                    continue;
                }

                int nextAttempt = count + 1;
                appendUnlocked(Map.of("event", "delivery", "consumerId", consumer, "messageId", m.messageId(), "attempt", nextAttempt, "at", now));
                attempts.put(k, nextAttempt);
                lastDelivery.put(k, now);
                result.add(new DeliveryCandidate(m, nextAttempt));
            }
        });

        return result;
    }

    public synchronized boolean ack(String consumer, String messageId) throws IOException {
        withFileLock(() -> {
            reload();

            if (!messages.containsKey(messageId)) {
                return;
            }

            String k = key(consumer, messageId);

            if (!acknowledgements.contains(k)) {
                appendUnlocked(Map.of("event", "ack", "consumerId", consumer, "messageId", messageId));
                acknowledgements.add(k);
            }
        });

        return messages.containsKey(messageId);
    }

    public synchronized boolean nack(String consumer, String messageId, String reason) throws IOException {
        reload();

        if (!messages.containsKey(messageId)) {
            return false;
        }

        String k = key(consumer, messageId);

        if (acknowledgements.contains(k) || deadLetters.contains(k)) {
            return false;
        }

        append(Map.of("event", "nack", "consumerId", consumer, "messageId", messageId, "reason", reason == null ? "" : reason, "at", System.currentTimeMillis()));
        lastDelivery.put(k, 0L);

        if (attempts.getOrDefault(k, 0) >= MAX_ATTEMPTS) {
            deadLetter(consumer, messageId, "maximum attempts reached after NACK");
        }

        return true;
    }

    private void deadLetter(String consumer, String id, String reason) throws IOException {
        withFileLock(() -> {
            reload();
            deadLetterUnlocked(consumer, id, reason);
        });
    }

    private void deadLetterUnlocked(String consumer, String id, String reason) throws IOException {
        String k = key(consumer, id);
        if (deadLetters.add(k)) {
            appendUnlocked(Map.of("event", "dlq", "consumerId", consumer, "messageId", id, "reason", reason, "at", Instant.now().toString()));
        }
    }

    public synchronized int messageCount() throws IOException {
        reload();
        return messages.size();
    }

    public synchronized int pendingCount() throws IOException {
        reload();
        int n = 0;

        for (StoredMessage m : messages.values()) {
            for (String s : subscriptions) {
                String[] p = s.split("\\u0000", 2);
                if (p.length == 2 && p[1].equals(m.topic())
                        && !acknowledgements.contains(key(p[0], m.messageId()))
                        && !deadLetters.contains(key(p[0], m.messageId()))) {
                    n++;
                }
            }
        }

        return n;
    }

    public synchronized int deadLetterCount() throws IOException {
        reload();
        return deadLetters.size();
    }

    public synchronized int subscriptionCount() throws IOException {
        reload();
        return subscriptions.size();
    }

    public synchronized Path backup(Path directory) throws IOException {
        Files.createDirectories(directory);
        Path target = directory.resolve("messages-backup-" + System.currentTimeMillis() + ".jsonl");
        withFileLock(() -> Files.copy(journal, target, StandardCopyOption.COPY_ATTRIBUTES));
        return target;
    }

    public synchronized void restore(Path backup) throws IOException {
        if (!Files.isRegularFile(backup)) {
            throw new IOException("Backup file not found: " + backup);
        }

        withFileLock(() -> {
            try {
                Files.copy(backup, journal, StandardCopyOption.REPLACE_EXISTING);
                reload();
            } catch (IOException e) {
                throw new RuntimeException("Failed to restore backup", e);
            }
        });
    }

    private void reload() throws IOException {
        messages.clear();
        acknowledgements.clear();
        attempts.clear();
        lastDelivery.clear();
        deadLetters.clear();
        subscriptions.clear();

        if (!Files.exists(journal)) {
            return;
        }

        for (String line : Files.readAllLines(journal, StandardCharsets.UTF_8)) {
            if (line.isBlank()) {
                continue;
            }

            Map<?, ?> e;
            try {
                e = mapper.readValue(line, Map.class);
            } catch (Exception badLine) {
                continue;
            }

            String event = String.valueOf(e.get("event"));

            if (event.equals("publish")) {
                StoredMessage m = mapper.convertValue(e.get("message"), StoredMessage.class);
                messages.putIfAbsent(m.messageId(), m);
            } else if (event.equals("ack")) {
                acknowledgements.add(key(str(e, "consumerId"), str(e, "messageId")));
            } else if (event.equals("delivery")) {
                String k = key(str(e, "consumerId"), str(e, "messageId"));
                attempts.put(k, ((Number) e.get("attempt")).intValue());
                lastDelivery.put(k, ((Number) e.get("at")).longValue());
            } else if (event.equals("nack")) {
                lastDelivery.put(key(str(e, "consumerId"), str(e, "messageId")), 0L);
            } else if (event.equals("dlq")) {
                deadLetters.add(key(str(e, "consumerId"), str(e, "messageId")));
            } else if (event.equals("subscribe")) {
                subscriptions.add(key(str(e, "consumerId"), str(e, "topic")));
            } else if (event.equals("unsubscribe")) {
                subscriptions.remove(key(str(e, "consumerId"), str(e, "topic")));
            }
        }
    }

    private void append(Object event) throws IOException {
        withFileLock(() -> appendUnlocked(event));
    }

    private void appendUnlocked(Object event) throws IOException {
        byte[] bytes = (mapper.writeValueAsString(event) + "\n").getBytes(StandardCharsets.UTF_8);

        try (FileChannel channel = FileChannel.open(journal, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
            channel.force(true);
        }
    }

    private void withFileLock(IOAction action) throws IOException {
        try (FileChannel lockChannel = FileChannel.open(lockPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
             FileLock ignored = lockChannel.lock()) {
            action.run();
        }
    }

    private static String str(Map<?, ?> m, String k) {
        return String.valueOf(m.get(k));
    }

    private static String key(String a, String b) {
        return a + "\u0000" + b;
    }

    @FunctionalInterface
    private interface IOAction {
        void run() throws IOException;
    }

    public static final class DuplicateMessageException extends IllegalArgumentException {
        public DuplicateMessageException(String m) {
            super(m);
        }
    }

    public record StoredMessage(String messageId, String topic, String payload, String messageType,
                                String correlationId, String occurredAt, String schemaVersion) {
    }

    public record DeliveryCandidate(StoredMessage message, int attempt) {
    }
}