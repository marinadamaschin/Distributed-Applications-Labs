package md.utm.messaging.broker;

import java.util.concurrent.*;

public final class ConsumerRegistry {
    private final ConcurrentMap<String, ConsumerConnection> consumers = new ConcurrentHashMap<>();

    public ConsumerConnection register(String d, ConsumerConnection c) {
        ConsumerConnection old = consumers.put(d, c);
        if (old != null && old != c) try {
            old.close();
        } catch (Exception ignored) {
        }
        return c;
    }

    public ConsumerConnection get(String d) {
        return consumers.get(d);
    }

    public void removeIfSame(String d, ConsumerConnection c) {
        consumers.remove(d, c);
    }

    public ConsumerConnection await(String d) throws InterruptedException {
        while (true) {
            var c = consumers.get(d);
            if (c != null) return c;
            Thread.sleep(100);
        }
    }
}
