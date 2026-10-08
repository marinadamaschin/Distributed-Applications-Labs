package md.utm.messaging.unit;

import md.utm.messaging.contracts.*;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

class MessageFactoryTest {
    @Test
    void createsRequiredMetadata() {
        var m = MessageFactory.create("OrderCreated", "orders", "{}", null);
        assertNotNull(m.id());
        assertNotNull(m.timestamp());
        assertNotNull(m.correlationId());
        assertEquals("1.0", m.schemaVersion());
        assertEquals("OrderCreated", m.type());
        assertEquals("orders", m.destination());
    }
}
