# Dovada scenariului de eșec - crash-before-ACK
## 1. Starea inițială
Broker pornit, `consumer-orders` înregistrat pentru `orders`; pentru test curat se șterg `processed-consumer-orders.txt` și `effects-consumer-orders.log`.
## 2. Injectarea eșecului
Consumer-ul pornește în modul `crash-before-ack`. După mesaj: execută efectul local, persistă `messageId`, apoi iese intenționat înainte de ACK.
## 3. Comportamentul observat
Broker: `DELIVERY attempt=1`; Consumer: `PROCESSED`, `CRASH_BEFORE_ACK`; Broker nu primește ACK și aplică retry.
## 4. Recuperare
Consumer-ul este repornit `normal`; Broker retrimite același `messageId` la `attempt=2`; dedup detectează duplicatul, emite `DUPLICATE result=effect-skipped`, trimite ACK, Broker emite `CONFIRMED`.
## 5. Starea finală
Mesajul este confirmat după redelivery, fără repetarea intenționată a efectului local.
## 6. Garanția
`at-least-once + retry limitat + deduplicare`.
## 7. Limitări
Nu este exactly-once; există fereastra efect-local → persistare ID; cozile Brokerului sunt in-memory și nu supraviețuiesc restartului complet.
## Verificare reproductibilă
Vezi `docs/runbook/demo.md`. Testul `DeduplicationTest` verifică persistența dedup după recrearea store-ului; scenariul complet de crash este manual și reproductibil.
