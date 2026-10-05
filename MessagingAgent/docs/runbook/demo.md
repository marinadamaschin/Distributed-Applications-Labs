# Ghid de reproducere și demonstrare
## Cerințe
JDK 17+, Maven 3.9+, port TCP 5000 liber.
## Build și teste
```powershell
mvn clean test
```
## Rulare din IDE
1. Rulează `md.utm.messaging.broker.BrokerApplication`.
2. Rulează `ConsumerApplication` cu argumente: `consumer-orders orders normal`.
3. Rulează încă un `ConsumerApplication`: `consumer-audit audit normal`.
4. Rulează `ProducerApplication`.

În fluxul normal se urmăresc evenimentele `BROKER_ACCEPTED`, `ACCEPTED`, `DELIVERY`, `RECEIVED`, `PROCESSED`, `ACK_SENT`, `ACK_RECEIVED`, `CONFIRMED`. Același `correlationId` leagă mesajele aceleiași operații; `messageId` identifică mesajul concret.

## NACK / Retry / DLQ
Oprește consumer-orders normal și pornește `ConsumerApplication` cu:
```text
consumer-orders orders always-nack
```
Rulează Producer. Pentru `orders` trebuie observate 3 livrări cu NACK și apoi `DLQ_MOVED`. NACK produce retry imediat. Destinația `audit` poate continua independent.

## Crash-before-ACK
1. Oprește consumer-orders.
2. Șterge `processed-consumer-orders.txt` și `effects-consumer-orders.log`.
3. Pornește consumer-orders cu `consumer-orders orders crash-before-ack`.
4. Rulează Producer. Consumer-ul execută efectul, persistă ID-ul și iese cu cod 17 înainte de ACK.
5. Repornește consumer-orders cu `consumer-orders orders normal`.
6. Brokerul retrimite același `messageId`; Consumer-ul emite `DUPLICATE`, omite efectul, trimite ACK; Brokerul emite `CONFIRMED`.

## Ce se demonstrează
`publishAccepted` != procesare confirmată. Sistemul oferă at-least-once, retry maxim 3, deduplicare, FIFO per destinație și DLQ. Nu oferă exactly-once și nu are cozi Broker durabile.
