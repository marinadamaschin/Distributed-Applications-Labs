package md.utm.messaging.producer;

import md.utm.messaging.contracts.MessageEnvelope;

import java.io.IOException;

public final class ReliableProducer {

    private final ProducerClient client;
    private final int maxAttempts;
    private final long retryDelayMillis;

    public ReliableProducer(
            ProducerClient client,
            int maxAttempts,
            long retryDelayMillis) {

        if (maxAttempts < 1 || retryDelayMillis < 0) {
            throw new IllegalArgumentException(
                    "Invalid retry configuration"
            );
        }

        this.client = client;
        this.maxAttempts = maxAttempts;
        this.retryDelayMillis = retryDelayMillis;
    }

    public boolean publish(MessageEnvelope message)
            throws IOException {

        IOException lastError = null;

        for (int attempt = 1;
             attempt <= maxAttempts;
             attempt++) {

            try {
                if (client.publish(message)) {
                    return true;
                }

                // Broker-ul a respins explicit mesajul.
                // Nu repetam automat o cerere invalida.
                return false;

            } catch (IOException e) {
                lastError = e;

                System.err.println(
                        "[PRODUCER RETRY] messageId="
                                + message.id()
                                + " attempt="
                                + attempt
                                + "/"
                                + maxAttempts
                                + " reason="
                                + e.getMessage()
                );

                if (attempt < maxAttempts) {
                    try {
                        Thread.sleep(retryDelayMillis);
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new IOException(
                                "Producer retry interrupted",
                                interrupted
                        );
                    }
                }
            }
        }

        throw new IOException(
                "Publishing failed after "
                        + maxAttempts
                        + " attempts for message "
                        + message.id(),
                lastError
        );
    }
}
