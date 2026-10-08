package md.utm.messaging.broker;

import md.utm.messaging.contracts.*;

import java.util.concurrent.*;
import java.util.*;

public final class BrokerEngine {
    private final BrokerStorage storage;
    private final ConsumerRegistry registry;
    private final BrokerOptions options;
    private final Set<String> dispatchers = ConcurrentHashMap.newKeySet();
    private final ExecutorService pool = Executors.newCachedThreadPool();

    public BrokerEngine(BrokerStorage s, ConsumerRegistry r, BrokerOptions o) {
        storage = s;
        registry = r;
        options = o;
    }

    public boolean publish(MessageEnvelope m) {
        if (!valid(m)) return false;
        storage.queue(m.destination()).add(m);
        StructuredLog.write("INFO", "broker", "ACCEPTED", m, "accepted", null);
        startDispatcher(m.destination());
        return true;
    }

    private boolean valid(MessageEnvelope m) {
        return m != null && m.id() != null && m.timestamp() != null && m.correlationId() != null && "1.0".equals(m.schemaVersion()) && m.type() != null && m.destination() != null && m.payload() != null;
    }

    private void startDispatcher(String d) {
        if (dispatchers.add(d)) pool.submit(() -> processQueue(d));
    }

    private void processQueue(String d) {
        try {
            while (!Thread.currentThread().isInterrupted()) {
                MessageEnvelope m = storage.queue(d).take();
                processMessage(m);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            dispatchers.remove(d);
        }
    }

    private void processMessage(MessageEnvelope m) throws InterruptedException {
        String last = "retry exhausted";
        for (int attempt = 1; attempt <= options.maxAttempts(); attempt++) {
            ConsumerConnection c = registry.await(m.destination());
            StructuredLog.write("INFO", "broker", "DELIVERY", m, null, "attempt=" + attempt + " consumer=" + c.consumerId());
            try {
                boolean ack = c.deliver(m, attempt);
                if (ack) {
                    StructuredLog.write("INFO", "broker", "ACK_RECEIVED", m, "success", "attempt=" + attempt);
                    StructuredLog.write("INFO", "broker", "CONFIRMED", m, "success", "attempt=" + attempt);
                    return;
                }
                last = "consumer NACK";
                StructuredLog.write("WARN", "broker", "NACK_RECEIVED", m, "retry", "attempt=" + attempt);
            } catch (Exception ex) {
                last = ex.getMessage();
                registry.removeIfSame(m.destination(), c);
                StructuredLog.write("WARN", "broker", "NO_ACK", m, "retry", "attempt=" + attempt + " reason=" + ex.getClass().getSimpleName());
                if (attempt < options.maxAttempts()) Thread.sleep(options.retryDelayMilliseconds());
            }
        }
        storage.deadLetter(new DeadLetter(m, options.maxAttempts(), last));
        StructuredLog.write("ERROR", "broker", "DLQ_MOVED", m, "dead-lettered", "attempts=" + options.maxAttempts());
    }

    public void shutdown() {
        pool.shutdownNow();
    }
}
