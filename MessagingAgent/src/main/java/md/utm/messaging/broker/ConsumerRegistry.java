package md.utm.messaging.broker;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class ConsumerRegistry {

    private final ConcurrentMap<String,
            ConcurrentMap<String, ConsumerConnection>> consumers =
            new ConcurrentHashMap<>();

    private static void closeQuietly(
            ConsumerConnection connection) {

        try {
            connection.close();
        } catch (IOException ignored) {

        }
    }

    public void register(
            String topic,
            ConsumerConnection connection) {

        if (topic == null || topic.isBlank()
                || connection == null
                || connection.consumerId() == null
                || connection.consumerId().isBlank()
                || !topic.equals(connection.destination())) {
            throw new IllegalArgumentException(
                    "Invalid subscriber registration");
        }

        ConcurrentMap<String, ConsumerConnection> topicConsumers =
                consumers.computeIfAbsent(
                        topic,
                        ignored -> new ConcurrentHashMap<>());

        ConsumerConnection previous = topicConsumers.put(
                connection.consumerId(),
                connection);

        // Reconectarea aceluiasi subscriber inlocuieste
        // doar conexiunea sa anterioara.
        if (previous != null && previous != connection) {
            closeQuietly(previous);
        }
    }

    public ConsumerConnection get(
            String topic,
            String subscriberId) {

        Map<String, ConsumerConnection> topicConsumers =
                consumers.get(topic);

        return topicConsumers == null
                ? null
                : topicConsumers.get(subscriberId);
    }

    public List<ConsumerConnection> getAll(String topic) {
        Map<String, ConsumerConnection> topicConsumers =
                consumers.get(topic);

        if (topicConsumers == null) {
            return List.of();
        }

        return List.copyOf(topicConsumers.values());
    }

    public void removeIfSame(
            String topic,
            ConsumerConnection connection) {

        if (connection == null) {
            return;
        }

        ConcurrentMap<String, ConsumerConnection> topicConsumers =
                consumers.get(topic);

        if (topicConsumers != null) {
            topicConsumers.remove(
                    connection.consumerId(),
                    connection);
        }
    }

    public int activeCount(String topic) {
        return getAll(topic).size();
    }

    public void closeAll() {
        for (Map<String, ConsumerConnection> topicConsumers
                : consumers.values()) {

            for (ConsumerConnection connection
                    : topicConsumers.values()) {
                closeQuietly(connection);
            }
            topicConsumers.clear();
        }

        consumers.clear();
    }

    public void disconnect(
            String topic,
            String subscriberId) {

        ConcurrentMap<String, ConsumerConnection> topicConsumers =
                consumers.get(topic);

        if (topicConsumers == null) {
            return;
        }

        ConsumerConnection connection =
                topicConsumers.remove(subscriberId);

        if (connection != null) {
            closeQuietly(connection);
        }
    }

}
