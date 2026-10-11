# Diagrama componentelor gRPC

```mermaid
flowchart LR
    Producer[GrpcProducerApplication] -->|Publish RPC| API[gRPC / HTTP2 + Protobuf]
    Consumer[GrpcConsumerApplication] -->|Subscribe / Ack / Nack RPC| API
    API --> Service[GrpcMessagingService]
    Service --> Store[FileMessageStore]
    Service --> Routing[Routing / subscription handling]
    Service --> Delivery[Delivery / retry / DLQ logic]
    Store --> Journal[(messages.jsonl)]
    Pool[GrpcEndpointPool] -->|round-robin / failover| API
    Tests[Automated tests] -. validate .-> Store
    Tests -. validate .-> Service
```

## Responsabilități

- **Producer:** publică mesaje prin clientul gRPC.
- **Consumer:** se abonează, primește mesaje și trimite ACK/NACK.
- **gRPC API:** contractul tipizat definit în `src/main/proto/messaging.proto`.
- **GrpcMessagingService:** adaptează apelurile RPC la operațiile brokerului.
- **FileMessageStore:** jurnalizează datele necesare recuperării.
- **GrpcEndpointPool:** selectează endpoint-uri client-side și încearcă failover-ul configurat.

Notă: componentele prezentate sunt separări logice; o diagramă nu constituie dovadă că toate politicile de livrare sunt
implementate complet.

```