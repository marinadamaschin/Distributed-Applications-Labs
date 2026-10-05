package md.utm.messaging.broker; public record BrokerOptions(int port,int maxAttempts,int retryDelayMilliseconds){public BrokerOptions(){this(5000,3,300);}}
