# Dovada scenariului de eșec — Crash Before ACK

## 1. Obiectivul testului

Scenariul verifică comportamentul sistemului atunci când un Consumer procesează un mesaj, execută efectul local și se oprește neașteptat înainte de transmiterea confirmării ACK.

Se urmăresc recuperarea livrării neconfirmate, retransmiterea mesajului și evitarea repetării efectului local prin deduplicare.

## 2. Starea inițială

- Broker-ul este pornit și utilizează persistența SQLite.
- Consumer-ul `consumer-orders` este abonat la topicul `orders`.
- Consumer-ul utilizează modul `crash-before-ack`.
- Pentru un test izolat se utilizează fișiere locale de deduplicare și efecte fără înregistrări anterioare pentru mesajul testat.

## 3. Injectarea eșecului

Producer-ul publică un mesaj nou pe topicul `orders`.

Broker-ul persistă mesajul și creează livrarea corespunzătoare.

Consumer-ul primește mesajul și execută următoarele operații:

1. Scrie efectul local în fișierul de evidență.
2. Salvează `messageId` în `DeduplicationStore`.
3. Se oprește intenționat cu codul de ieșire `17`, înainte de transmiterea ACK-ului.

Astfel este simulată oprirea procesului după realizarea efectului local, dar înainte de confirmarea procesării către Broker.

## 4. Comportamentul așteptat

La prima livrare se urmăresc evenimentele:

- Broker: `DELIVERY`, cu identificatorul mesajului și numărul încercării.
- Consumer: `PROCESSED`.
- Consumer: `CRASH_BEFORE_ACK`.
- Broker: detectarea pierderii conexiunii sau a lipsei confirmării.

Broker-ul păstrează starea livrării în SQLite și poate programa o nouă încercare.

Dacă Broker-ul este repornit cu o livrare rămasă `IN_FLIGHT`, mecanismul de recuperare permite reintroducerea acesteia în procesul de distribuire.

## 5. Recuperarea

Consumer-ul este repornit în modul `normal`, utilizând același `consumerId` și aceleași fișiere locale.

La retransmiterea mesajului:

1. Consumer-ul primește același `messageId`.
2. Verifică identificatorul în `DeduplicationStore`.
3. Identifică mesajul ca fiind deja procesat.
4. Omite repetarea efectului local.
5. Trimite ACK către Broker.
6. Broker-ul persistă starea `ACKED`.

În loguri se urmăresc evenimentele `DUPLICATE`, `ACK_SENT` și `CONFIRMED`.

## 6. Starea finală așteptată

- Mesajul rămâne persistat în SQLite.
- Livrarea este confirmată prin starea `ACKED`.
- Efectul local nu este repetat în cazul retransmiterii detectate ca duplicat.
- Identificatorul mesajului rămâne înregistrat în mecanismul local de deduplicare.

## 7. Garanția demonstrată

Scenariul ilustrează combinația:

**At-least-once + retry limitat + persistență + deduplicare.**

Broker-ul poate retransmite mesajele neconfirmate, iar Consumer-ul poate evita repetarea efectelor pentru mesajele deja înregistrate.

## 8. Limitări

Sistemul nu garantează procesarea exactly-once.

Există un interval între executarea efectului local și salvarea identificatorului în mecanismul de deduplicare. Dacă procesul se oprește în acel interval, efectul poate fi repetat la retransmitere.

Mesajele și stările livrărilor sunt persistate în SQLite, însă fișierele locale ale Consumer-ului nu sunt coordonate tranzacțional cu baza de date a Broker-ului.

Numărul maxim de încercări este limitat. Dacă acesta este epuizat, livrarea poate ajunge în starea `DEAD_LETTER`, fără retransmitere automată ulterioară.

## 9. Verificare și dovezi

Pașii de reproducere sunt descriși în `docs/runbook/demo.md`.

Testul automat `DeduplicationTest` verifică păstrarea informațiilor de deduplicare după recrearea store-ului.

Scenariul complet de oprire a procesului înainte de ACK este demonstrat manual. Pentru documentarea execuției se pot include fragmente reale din logurile Broker-ului și Consumer-ului, precum și starea finală din SQLite.

Aceste dovezi trebuie colectate dintr-o execuție efectivă, nu reconstruite din rezultatul așteptat.