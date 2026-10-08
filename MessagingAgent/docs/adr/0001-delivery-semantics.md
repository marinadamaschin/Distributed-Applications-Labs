# ADR 0001 — Alegerea semanticii de livrare și a mecanismului de persistență

## 1. Context

Sistemul implementează comunicarea asincronă de tip Publish–Subscribe între mai mulți Producer-i și Consumer-i, prin intermediul unui Broker TCP.

Într-un sistem distribuit, pot apărea situații precum:

- pierderea conexiunii dintre Broker și Consumer;
- oprirea neașteptată a unui Consumer în timpul procesării;
- procesarea mesajului fără transmiterea confirmării ACK;
- indisponibilitatea temporară a Broker-ului;
- repornirea Broker-ului în timp ce există livrări neconfirmate.

Prin urmare, sistemul trebuie să păstreze mesajele și stările livrărilor și să permită recuperarea după anumite tipuri de eșec.

## 2. Decizie arhitecturală

A fost aleasă semantica de livrare **at-least-once**, utilizând ACK/NACK, retry limitat, persistență SQLite și deduplicare la nivelul Consumer-ului.

### 2.1. Confirmarea publicării

Broker-ul validează și persistă mesajul înainte să trimită răspunsul `publishAccepted` către Producer.

Confirmarea publicării indică acceptarea și persistarea mesajului, nu procesarea acestuia de către Consumer.

### 2.2. Confirmarea procesării

Pentru fiecare Subscriber se creează o înregistrare independentă de livrare.

- `ACK` — Consumer-ul confirmă procesarea, iar Broker-ul persistă starea `ACKED`.
- `NACK` — procesarea a eșuat, iar Broker-ul poate programa o nouă încercare.
- Lipsa ACK-ului sau pierderea conexiunii — Broker-ul tratează livrarea ca nereușită și aplică mecanismul de recuperare sau retry.

Numărul maxim de încercări este configurabil, valoarea implicită fiind **3**.

După epuizarea încercărilor, livrarea este marcată `DEAD_LETTER` în SQLite.

### 2.3. Persistența

A fost utilizată baza de date relațională SQLite pentru stocarea:

- mesajelor în tabela `messages`;
- abonamentelor în tabela `subscriptions`;
- livrărilor și stărilor acestora în tabela `deliveries`.

Persistarea mesajului și crearea livrărilor aferente se realizează tranzacțional.

La repornirea Broker-ului, livrările rămase `IN_FLIGHT` pot fi recuperate și reintroduse în mecanismul de distribuire.

Sistemul include și operații de backup periodic și restaurare a bazei de date.

### 2.4. Deduplicarea

Consumer-ul utilizează `DeduplicationStore` pentru a păstra identificatorii mesajelor deja procesate.

Dacă același `messageId` este primit din nou, Consumer-ul evită repetarea efectului local și transmite ACK.

Acest mecanism reduce efectele retransmiterilor, dar nu garantează procesarea exactly-once în toate scenariile.

## 3. Alternative analizate

### At-most-once

Mesajul este transmis fără mecanism de retransmitere care să garanteze o nouă încercare după eșec.

**Avantaj:** implementare mai simplă și posibilitatea unui număr mai mic de transmisii.

**Dezavantaj:** mesajele pot fi pierdute dacă procesarea eșuează.

Această alternativă nu a fost aleasă deoarece proiectul urmărește recuperarea livrărilor neconfirmate.

### Exactly-once

Urmărește evitarea procesării duplicate, inclusiv în scenarii de eșec.

**Avantaj:** garanții mai puternice privind efectele procesării.

**Dezavantaj:** necesită mecanisme suplimentare de coordonare și atomicitate între procesarea mesajului și înregistrarea efectului produs.

ACK-ul și deduplicarea bazată pe fișiere nu sunt suficiente pentru a garanta exactly-once.

### Stocare exclusiv în memorie

Ar permite o implementare mai simplă, dar mesajele și stările livrărilor ar putea fi pierdute la oprirea Broker-ului.

Această abordare a fost înlocuită cu persistența SQLite.

## 4. Justificarea utilizării SQLite

SQLite a fost aleasă deoarece:

- permite persistență locală fără un server separat de baze de date;
- oferă tranzacții pentru actualizările mesajelor și livrărilor;
- simplifică instalarea și demonstrarea laboratorului;
- permite inspectarea directă a stărilor mesajelor;
- oferă mecanisme pentru backup și verificarea integrității bazei de date.

Soluția este potrivită pentru un proiect didactic și un Broker de dimensiuni reduse.

Pentru sisteme distribuite la scară mare, cu cerințe ridicate de disponibilitate și debit, ar trebui analizate alternative precum un Broker dedicat și o infrastructură de persistență distribuită.

## 5. Consecințe

### Avantaje

- mesajele acceptate sunt persistate;
- fiecare Subscriber are propria stare de livrare;
- există retry și DLQ persistentă;
- Broker-ul poate recupera livrări după restart;
- Consumer-ul poate evita repetarea efectelor pentru mesaje deja înregistrate;
- sistemul poate fi verificat prin teste automate și scenarii de eșec.

### Compromisuri

- același mesaj poate fi transmis de mai multe ori;
- sunt necesare operații suplimentare de citire și scriere în SQLite;
- concurența la scriere este limitată de caracteristicile SQLite;
- retransmiterile pot modifica ordinea observată a procesării;
- disponibilitatea Broker-ului depinde de procesul și stocarea locală, neexistând replicare automată între mai multe noduri.

## 6. Limitări cunoscute

Sistemul nu garantează exactly-once.

Există o fereastră între executarea efectului local și salvarea identificatorului mesajului în mecanismul de deduplicare. Dacă procesul se oprește în acest interval, efectul poate fi repetat la retransmitere.

Nu este garantată ordinea globală strict FIFO a procesării mesajelor, în special în prezența retry-urilor, reconectărilor și a mai multor Subscriber-i.

Dezabonarea și reconectarea simultană pot necesita mecanisme suplimentare de coordonare pentru eliminarea tuturor condițiilor de cursă.

## 7. Concluzie

Alegerea semanticii at-least-once, combinată cu SQLite, ACK/NACK, retry, DLQ și deduplicare, reprezintă un compromis între fiabilitate, complexitate și ușurința demonstrării.

Arhitectura permite studierea unor probleme reale ale sistemelor distribuite, fără introducerea unei infrastructuri externe complexe.