# MessagingAgent — Sistem distribuit de mesagerie Publish–Subscribe

Proiect Java care implementează un sistem de mesagerie distribuită bazat pe modelul **Producer–Broker–Consumer**, utilizând socket-uri TCP, mesaje JSON și persistență SQLite.

Proiectul demonstrează comunicarea asincronă între aplicații independente, distribuirea mesajelor către mai mulți Subscriber-i, tratarea erorilor și recuperarea livrărilor neconfirmate.

## 1. Arhitectură

Sistemul conține trei aplicații executabile separat:

- **Producer** — creează și publică mesaje către Broker.
- **Broker** — primește mesajele, le persistă, gestionează abonamentele și coordonează livrările.
- **Consumer** — primește și procesează mesajele, transmite ACK/NACK și detectează duplicatele.

Componentele sunt organizate în package-uri în cadrul unui singur proiect Maven, dar rulează în procese separate și comunică prin TCP.

Broker-ul utilizează SQLite pentru păstrarea mesajelor, abonamentelor și stărilor livrărilor.

Diagrama arhitecturii este disponibilă în [docs/diagrams/architecture.md](docs/diagrams/architecture.md).

## 2. Funcționalități implementate

- Publicare de mesaje JSON prin TCP.
- Mai mulți Producer-i și Consumer-i concurenți.
- Abonare și dezabonare dinamică.
- Mai mulți Subscriber-i pentru același topic.
- Livrări independente pentru fiecare Subscriber.
- Confirmări ACK/NACK.
- Timeout pentru lipsa ACK-ului.
- Retry limitat, cu maximum 3 încercări în configurația implicită.
- Persistarea mesajelor și a stărilor livrărilor în SQLite.
- DLQ persistentă, reprezentată prin starea `DEAD_LETTER`.
- Recuperarea livrărilor `IN_FLIGHT` după restartul Broker-ului.
- Reconectarea automată a Consumer-ului după întreruperea conexiunii.
- Retransmiterea publicării de către Producer în cazul erorilor de comunicare.
- Deduplicare după `messageId` la nivelul Consumer-ului.
- Backup periodic și restaurarea bazei de date.
- Teste automate și scenarii de simulare a eșecurilor.

## 3. Structura proiectului

```text
MessagingAgent/
├── pom.xml
├── README.md
├── .env.example
├── docs/
│   ├── adr/
│   ├── diagrams/
│   ├── evidence/
│   └── runbook/
├── scripts/
│   └── test-all.ps1
└── src/
    ├── main/java/md/utm/messaging/
    │   ├── broker/
    │   ├── consumers/
    │   ├── contracts/
    │   ├── orders/
    │   ├── persistence/
    │   └── producer/
    └── test/java/md/utm/messaging/
        ├── contract/
        ├── failure/
        ├── integration/
        ├── persistence/
        └── unit/
```

Responsabilitățile package-urilor:

- `broker` — server TCP, coordonarea livrărilor și gestionarea conexiunilor.
- `consumers` — procesarea mesajelor, deduplicare și simularea eșecurilor.
- `contracts` — structura mesajelor, protocol JSON și logging.
- `orders` — construirea mesajelor din domeniul comenzilor.
- `persistence` — acces SQLite, repository-uri, backup și restaurare.
- `producer` — publicarea mesajelor și retransmiterea în caz de eșec.

## 4. Contractul mesajelor

Mesajele conțin:

- `messageId` — identificator unic al mesajului;
- `occurredAt` — data și ora producerii evenimentului;
- `correlationId` — identificator pentru corelarea operațiilor;
- `schemaVersion` — versiunea contractului;
- `messageType` — tipul mesajului;
- `destination` — topicul către care se publică;
- `payload` — datele mesajului.

Protocolul utilizează JSON UTF-8, cu câte un cadru delimitat prin newline.

Operațiile principale includ `publish`, `publishAccepted`, `registerConsumer`, `registered`, `delivery`, `ack`, `nack` și `unsubscribe`.

## 5. Semantica livrării

Sistemul urmărește semantica **at-least-once**.

Broker-ul persistă mesajul înainte să confirme acceptarea publicării prin `publishAccepted`.

Pentru fiecare Subscriber activ se creează o livrare independentă, cu stare proprie.

Stările posibile ale unei livrări sunt:

- `PENDING`
- `IN_FLIGHT`
- `ACKED`
- `RETRY_PENDING`
- `DEAD_LETTER`
- `CANCELLED`

În cazul lipsei ACK-ului sau al unui NACK, Broker-ul poate reîncerca livrarea. După epuizarea încercărilor, livrarea este marcată `DEAD_LETTER`.

Consumer-ul utilizează deduplicarea pentru evitarea repetării efectelor locale ale mesajelor deja înregistrate.

**Limitări:** sistemul nu garantează exactly-once și nici o ordine globală strict FIFO în prezența retransmiterilor și reconectărilor.

Justificarea alegerilor este prezentată în [ADR 0001](docs/adr/0001-delivery-semantics.md).

## 6. Cerințe de rulare

- Java JDK 17+
- Maven
- IntelliJ IDEA sau terminal
- Portul TCP `5000` disponibil
- Acces de scriere pentru baza de date SQLite și fișierele locale

## 7. Compilare și teste

Din directorul proiectului `MessagingAgent`:

```powershell
mvn clean test
```

La ultima verificare au trecut toate cele **18 teste automate**.

Testele acoperă contractele JSON, persistarea mesajelor, distribuirea către Subscriber-i, publicarea concurentă, deduplicarea, recuperarea livrărilor, DLQ, backup și restaurare.

## 8. Rulare din IntelliJ IDEA

Se pornesc separat următoarele clase:

1. `md.utm.messaging.broker.BrokerApplication`
2. `md.utm.messaging.consumers.ConsumerApplication`
3. O a doua instanță `ConsumerApplication`
4. `md.utm.messaging.producer.ProducerApplication`

Argumente pentru Consumer-i:

```text
consumer-orders orders normal
consumer-audit audit normal
```

Pentru demonstrarea scenariilor `always-nack`, `crash-before-ack` și a recuperării după eșec, consultați [ghidul de demonstrare](docs/runbook/demo.md).

## 9. Scenarii de testare

Sistemul permite demonstrarea următoarelor situații:

- Publicare și procesare normală.
- Doi Subscriber-i care primesc independent același mesaj.
- Dezabonarea unui Consumer.
- NACK urmat de retry și DLQ.
- Crash al Consumer-ului după efectul local, dar înainte de ACK.
- Restartul Broker-ului și recuperarea livrărilor persistate.
- Indisponibilitatea Broker-ului și retransmiterea publicării.
- Backup și restaurarea bazei de date.

## 10. Documentație

- [Arhitectura sistemului](docs/diagrams/architecture.md)
- [Diagrama de secvență — Crash Before ACK](docs/diagrams/sequence.md)
- [ADR — Semantica livrării](docs/adr/0001-delivery-semantics.md)
- [Dovada scenariului Crash Before ACK](docs/evidence/crash-before-ack.md)
- [Ghid de reproducere și demonstrare](docs/runbook/demo.md)

## 11. Concluzie

Proiectul demonstrează funcționarea unui sistem Publish–Subscribe cu persistență, livrări independente, confirmări, retry și mecanisme de recuperare.

Arhitectura a fost proiectată pentru a evidenția probleme reale ale aplicațiilor distribuite și compromisurile dintre fiabilitate, complexitate și performanță.