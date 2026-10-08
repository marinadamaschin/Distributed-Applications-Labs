package md.utm.messaging.integration;

import md.utm.messaging.broker.*;
import md.utm.messaging.contracts.*;
import md.utm.messaging.producer.*;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

import java.util.concurrent.*;

class PublishAcceptanceTest {
    @Test
    void producerGetsAcceptanceWithoutConsumerAck() throws Exception {
        int port = 15101;
        var o = new BrokerOptions(port, 2, 50);
        var storage = new BrokerStorage();
        var registry = new ConsumerRegistry();
        var engine = new BrokerEngine(storage, registry, o);
        var server = new BrokerServer(o, engine, registry);
        var ex = Executors.newSingleThreadExecutor();
        try {
            ex.submit(() -> {
                try {
                    server.run();
                } catch (Exception ignored) {
                }
            });
            Thread.sleep(150);
            var producer = new ProducerClient("127.0.0.1", port);
            var m = MessageFactory.create("Test", "nobody", "{}", "corr-test");
            assertTrue(producer.publish(m));
        } finally {
            server.close();
            ex.shutdownNow();
        }
    }
}
