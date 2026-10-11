# gRPC failure scenarios — evidence checklist

Do not mark a scenario as passed until it has been run and its logs/journal attached.

## 1. Broker restart before ACK

- Initial state: empty journal, broker started, consumer subscribed to `orders`.
- Inject failure: publish a message, stop broker after delivery but before ACK.
- Expected observation: the publish and delivery events remain in JSONL; after restart the unacknowledged message is
  delivered again.
- Recovery: consumer processes and ACKs the redelivery.
- Final state: persistent ACK event exists and no further delivery is emitted for that consumer/message.
- Guarantee: at-least-once, not exactly-once.
- Not covered: host/storage loss or corrupted journal records.

## 2. Retry and DLQ

- Initial state: consumer subscribed and message published.
- Inject failure: NACK each delivery or leave it unacknowledged through the retry windows.
- Expected observation: delivery attempts are persisted, delay increases, then a `dlq` event appears after the
  configured limit.
- Recovery: inspect the journal and status RPC.
- Final state: message remains in history and is excluded from normal pending delivery for that consumer.
- Not covered: administrative redrive; not implemented in this increment.

## 3. Broker endpoint failure

- Initial state: two broker endpoints configured with the same shared journal.
- Inject failure: stop one endpoint, publish using `GRPC_ENDPOINTS`.
- Expected observation: client retries the other endpoint using the same message ID.
- Recovery: available broker accepts the idempotent publish.
- Final state: one logical publish event for the message ID.
- Not covered: loss of shared storage, split-brain storage, or filesystems with unreliable locks.