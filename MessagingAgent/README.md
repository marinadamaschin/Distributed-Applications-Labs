# MessagingAgentLab - Java

Laborator: agent de mesagerie implementat cu socket-uri TCP și JSON.

## Arhitectură

Sistemul folosește modelul Producer -> Broker -> Consumer. Componentele sunt organizate ca package-uri într-un singur proiect Maven, dar aplicațiile `ProducerApplication`, `BrokerApplication` și `ConsumerApplication` se rulează separat și comunică prin TCP.

## Structură

```text
src/main/java/md/utm/messaging/
├── contracts/   # contractul mesajelor, protocol JSON și logging
├── producer/    # publicarea mesajelor
├── broker/      # validare, cozi, rutare, ACK/NACK, retry și DLQ
├── consumers/   # procesare, deduplicare și scenarii de failure
└── orders/      # construirea mesajelor de domeniu

src/test/java/md/utm/messaging/
├── unit/
├── contract/
├── integration/
└── failure/
```

## Contract

Mesajele includ `messageId`, `occurredAt` UTC, `correlationId`, `schemaVersion`, `messageType`, `destination` și `payload`. Protocolul folosește cadre JSON UTF-8 delimitate prin newline: `publish`, `publishAccepted`, `registerConsumer`, `registered`, `delivery`, `ack`, `nack`, `error`.

## Garanții și limite

- TCP + JSON.
- Rutare după `destination`; demonstrația folosește `orders` și `audit`.
- Un consumer activ per destinație.
- FIFO per destinație, nu global.
- At-least-once cu maximum 3 încercări.
- ACK confirmă procesarea; NACK sau lipsa ACK poate produce retry.
- După epuizarea încercărilor mesajul este mutat în DLQ.
- Consumer-ul deduplică după `messageId` și persistă identificatorii procesați.
- Cozile Brokerului și DLQ sunt tranzitorii (in-memory) și nu supraviețuiesc restartului Brokerului.
- Soluția nu pretinde exactly-once.

## Build și teste

Necesită Java 17+ și Maven.

```powershell
mvn clean test
```

## Rulare în IntelliJ IDEA

Rulați separat clasele cu `main()`:

1. `md.utm.messaging.broker.BrokerApplication`
2. `md.utm.messaging.consumers.ConsumerApplication` pentru `consumer-orders`
3. `md.utm.messaging.consumers.ConsumerApplication` pentru `consumer-audit`
4. `md.utm.messaging.producer.ProducerApplication`

Pentru scenariile `normal`, `always-nack` și `crash-before-ack`, consultați `docs/runbook/demo.md`.

## Documentație

- `docs/diagrams/architecture.md`
- `docs/diagrams/sequence.md`
- `docs/adr/0001-delivery-semantics.md`
- `docs/evidence/crash-before-ack.md`
- `docs/runbook/demo.md`
