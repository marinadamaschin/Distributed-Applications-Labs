# Ghid de reproducere și demonstrare

## 1. Cerințe

Pentru compilarea și executarea proiectului sunt necesare:

- JDK 17 sau o versiune compatibilă;
- Maven 3.9+;
- IntelliJ IDEA sau un terminal;
- portul TCP `5000` disponibil;
- acces de scriere în directorul de lucru pentru baza de date SQLite, backup-uri și fișierele Consumer-ilor.

Proiectul este implementat în Java și utilizează Maven pentru gestionarea dependențelor și executarea testelor.

## 2. Compilare și testare

Din directorul `MessagingAgent`, executăm:

```powershell
mvn clean test
```

Rezultatul așteptat este `BUILD SUCCESS`, cu 18 teste automate trecute.

Testele verifică, printre altele, contractele mesajelor, persistența, publicarea concurentă, distribuirea către mai mulți Subscriber-i, ACK-urile independente, retry, DLQ, backup și restaurare.

## 3. Pornirea sistemului din IntelliJ

Pornim aplicațiile în această ordine:

1. `md.utm.messaging.broker.BrokerApplication`
2. `md.utm.messaging.consumers.ConsumerApplication`, cu argumentele `consumer-orders orders normal`
3. A doua instanță `ConsumerApplication`, cu argumentele `consumer-audit audit normal`
4. `md.utm.messaging.producer.ProducerApplication`

Broker-ul ascultă conexiunile TCP și utilizează SQLite pentru stocarea mesajelor și a stărilor livrărilor.

Consumer-ii se înregistrează pentru topicurile corespunzătoare, iar Producer-ul publică mesajele scenariului practic.

În consolă urmărim evenimentele de acceptare, livrare, procesare și confirmare.

`messageId` identifică mesajul concret, iar `correlationId` permite asocierea mesajelor care aparțin aceleiași operații.

## 4. Demonstrarea mai multor Subscriber-i

Pentru a demonstra distribuirea Publish–Subscribe:

1. Pornim Broker-ul.
2. Pornim `consumer-orders` pe topicul `orders`.
3. Pornim un al doilea Consumer, cu identificator diferit, dar tot pe topicul `orders`.
4. Rulăm Producer-ul.
5. Verificăm că ambii Consumer-i primesc mesajul și trimit ACK independent.

În SQLite, pentru fiecare mesaj publicat pe `orders`, trebuie să existe câte o înregistrare de livrare pentru fiecare Subscriber activ în momentul publicării.

Această funcționalitate este verificată și printr-un test automat de integrare.

## 5. Demonstrarea NACK, Retry și DLQ

1. Pornim Broker-ul.
2. Oprim instanța normală `consumer-orders`.
3. Pornim `ConsumerApplication` cu argumentele:

```text
consumer-orders orders always-nack
```

4. Rulăm Producer-ul.
5. Urmărim încercările de livrare și răspunsurile NACK.

Cu configurația implicită, Broker-ul permite maximum trei încercări.

Între încercări se aplică întârzierea de retry configurată.

După epuizarea încercărilor, livrarea este marcată `DEAD_LETTER` în SQLite, iar Broker-ul afișează un mesaj `[DLQ]`.

Starea `DEAD_LETTER` rămâne disponibilă și după repornirea Broker-ului.

## 6. Demonstrarea scenariului Crash Before ACK

Pentru un test controlat, folosim un identificator Consumer și fișiere locale dedicate, astfel încât să nu afectăm datele altor demonstrații.

1. Pornim Broker-ul.
2. Pornim Consumer-ul în modul `crash-before-ack`.
3. Publicăm un mesaj nou.
4. Consumer-ul execută efectul local, înregistrează `messageId` și se oprește intenționat cu codul 17 înainte de transmiterea ACK-ului.
5. Broker-ul detectează lipsa confirmării sau pierderea conexiunii și păstrează starea necesară recuperării.
6. Repornim Consumer-ul cu același identificator, în modul `normal`, utilizând aceleași fișiere de deduplicare.
7. La retransmiterea mesajului, Consumer-ul identifică `messageId` deja procesat.
8. Efectul local nu este repetat, iar Consumer-ul trimite ACK.

Înainte de demonstrație trebuie verificat că livrarea nu a epuizat deja numărul maxim de încercări și nu a ajuns în starea `DEAD_LETTER`.

Acest scenariu demonstrează semantica at-least-once și rolul deduplicării.

## 7. Demonstrarea indisponibilității Broker-ului

1. Oprim Broker-ul.
2. Rulăm `ProducerApplication`.
3. Observăm încercările repetate de conectare și publicare.
4. După epuizarea încercărilor, Producer-ul raportează eșecul.
5. Repornim Broker-ul și rulăm din nou Producer-ul.
6. Verificăm acceptarea mesajelor.

Mecanismul `ReliableProducer` efectuează maximum trei încercări, păstrând același identificator al mesajului în cadrul retransmiterilor.

## 8. Demonstrarea reconectării Consumer-ului

1. Pornim Broker-ul și un Consumer în modul `normal`.
2. Oprim Broker-ul.
3. Observăm că Consumer-ul detectează pierderea conexiunii.
4. Repornim Broker-ul.
5. Consumer-ul încearcă reconectarea și reînregistrarea automată.
6. Publicăm un mesaj nou și verificăm procesarea acestuia.

Reconectarea este destinată întreruperilor neașteptate, nu dezabonării voluntare.

## 9. Demonstrarea persistenței și a recuperării

1. Pornim Broker-ul și publicăm mesaje.
2. Verificăm că acestea sunt salvate în SQLite.
3. Oprim Broker-ul.
4. Repornim aplicația utilizând aceeași bază de date.
5. Verificăm păstrarea mesajelor, abonamentelor și stărilor livrărilor.

La inițializare, Broker-ul recuperează livrările rămase `IN_FLIGHT`, astfel încât acestea să poată fi reîncercate.

Sistemul include backup periodic și restaurare, verificate prin teste automate.

## 10. Rezultatele demonstrate

Implementarea evidențiază:

- comunicare Publisher–Broker–Consumer prin TCP și JSON;
- publicare și distribuire concurentă;
- mai mulți Subscriber-i pentru același topic;
- abonare și dezabonare dinamică;
- ACK/NACK și retry limitat;
- DLQ persistentă;
- recuperare după restart;
- backup periodic și restaurare;
- deduplicare la nivelul Consumer-ului;
- diferența dintre `publishAccepted` și confirmarea procesării.

Sistemul urmărește semantica **at-least-once**, fără a garanta exactly-once sau ordonare globală strict FIFO.

## 11. Concluzie

Scenariile de demonstrare permit observarea comportamentului sistemului în condiții normale și în situații de eșec.

Utilizarea SQLite, a confirmărilor ACK/NACK și a mecanismelor de recuperare permite justificarea alegerilor arhitecturale și evaluarea funcționalității prin teste reproductibile.