package md.utm.messaging.consumers;

public enum ConsumerMode {
    NORMAL, ALWAYS_NACK, CRASH_BEFORE_ACK;

    public static ConsumerMode parse(String s) {
        return switch (s.toLowerCase()) {
            case "always-nack" -> ALWAYS_NACK;
            case "crash-before-ack" -> CRASH_BEFORE_ACK;
            default -> NORMAL;
        };
    }
}
