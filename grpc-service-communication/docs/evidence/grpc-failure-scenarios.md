# gRPC Messaging — Scenarii de fiabilitate și dovezi

Acest document rezumă rezultatele observate ale scenariilor de fiabilitate pentru mesageria gRPC, pe baza jurnalului brokerului și a logurilor consumatorului.

## Rezumat

| Scenariu | Rezultat observat | Status |
|---|---|---|
| Cădere de conexiune înainte de ACK | Reconectare, a doua livrare, ACK reușit | Observat |
| Retry și Dead-Letter Queue (DLQ) | Trei NACK-uri urmate de un eveniment DLQ | Observat |
| Mai multe endpointuri de broker | Două brokere pornite; comutarea reușită între endpointuri nu este demonstrată | Verificat parțial |
| Repornirea procesului broker și recuperarea jurnalului | Nedemonstrat concludent de logurile furnizate | Necesită test suplimentar |
| Concurența pe jurnalul partajat | Accesul concurent sigur nu este demonstrat | Necesită test suplimentar |

**Garanție de livrare:** comportamentul observat corespunde livrării *at-least-once*, nu procesării *exactly-once*. Consumatorii trebuie să fie **idempotenți**, deoarece pot primi livrări repetate.

---

## 1. Cădere de conexiune înainte de ACK

**Obiectiv:** Verificarea faptului că un mesaj neconfirmat poate fi livrat din nou după ce consumatorul pierde conexiunea.

### Procedură

1. Pornește brokerul gRPC.
2. Abonează un consumator la topicul `orders`.
3. Publică un mesaj `OrderCreated`.
4. Întrerupe conexiunea consumatorului înainte ca mesajul să fie confirmat.
5. Restabilește conexiunea și observă dacă mesajul este livrat din nou.
6. Verifică dacă consumatorul confirmă (ACK) în cele din urmă mesajul.

### Dovezi observate

Logurile consumatorului arată o eroare de conexiune urmată de reconectare:

```text
[CONSUMER] gRPC stream failed: Status{code=UNAVAILABLE, description=Network closed for unknown reason, cause=null}. Retry in 500 ms
[CONSUMER] Connecting to a configured broker endpoint...
```

Același mesaj a fost livrat de două ori:

```text
Received id=264924fa-9d2d-49f0-a6e3-b2f851f6e739 topic=orders type=OrderCreated payload={"orderId":"ORD-1001","amount":42.50} attempt=1
ACK/NACK failed; message may be redelivered: UNAVAILABLE: io exception
Received id=264924fa-9d2d-49f0-a6e3-b2f851f6e739 topic=orders type=OrderCreated payload={"orderId":"ORD-1001","amount":42.50} attempt=2
ACK persisted: 264924fa-9d2d-49f0-a6e3-b2f851f6e739
```

### Rezultat

**Observat:** reconectare, redelivery și ACK reușit.

Același `messageId` apare în ambele livrări. Prima încercare de ACK/NACK a eșuat deoarece conexiunea gRPC era indisponibilă; a doua livrare a fost confirmată cu succes.

> Aceste loguri demonstrează recuperarea după o cădere de conexiune. Ele **nu** dovedesc, în sine, că procesul brokerului a fost repornit. Un test separat de restart al brokerului trebuie să includă loguri de oprire/pornire și dovezi de recuperare a jurnalului.

---

## 2. Retry și Dead-Letter Queue (DLQ)

**Obiectiv:** Verificarea faptului că mesajele respinse în mod repetat sunt reîncercate și, în final, mutate în DLQ după atingerea limitei de încercări configurate.

### Procedură

1. Pornește brokerul și abonează un consumator la `orders`.
2. Publică un mesaj.
3. Configurează sau rulează consumatorul astfel încât să trimită NACK la fiecare livrare.
4. Observă numărul de încercări de livrare și întârzierile dintre retry-uri.
5. Verifică dacă brokerul înregistrează un eveniment DLQ după numărul maxim de încercări.

### Dovezi observate

Jurnalul înregistrează trei livrări și trei evenimente NACK pentru același mesaj:

```json
{"event":"delivery","consumerId":"consumer-c","messageId":"b804f6af-bb99-43c2-bcf9-04ca6f8e47a3","attempt":1}
{"event":"nack","reason":"failure injection: always-nack","consumerId":"consumer-c","messageId":"b804f6af-bb99-43c2-bcf9-04ca6f8e47a3"}
{"event":"delivery","consumerId":"consumer-c","messageId":"b804f6af-bb99-43c2-bcf9-04ca6f8e47a3","attempt":2}
{"event":"nack","reason":"failure injection: always-nack","consumerId":"consumer-c","messageId":"b804f6af-bb99-43c2-bcf9-04ca6f8e47a3"}
{"event":"delivery","consumerId":"consumer-c","messageId":"b804f6af-bb99-43c2-bcf9-04ca6f8e47a3","attempt":3}
{"event":"nack","reason":"failure injection: always-nack","consumerId":"consumer-c","messageId":"b804f6af-bb99-43c2-bcf9-04ca6f8e47a3"}
{"event":"dlq","reason":"maximum attempts reached after NACK","consumerId":"consumer-c","messageId":"b804f6af-bb99-43c2-bcf9-04ca6f8e47a3"}
```

Motivul respingerii este `failure injection: always-nack`, iar motivul DLQ este `maximum attempts reached after NACK`.

### Rezultat

**Observat:** retry-uri urmate de un eveniment DLQ după a treia livrare.

Jurnalul susține concluzia că brokerul a oprit retry-urile normale după limita de încercări configurată și a înregistrat mesajul în DLQ.

> Pentru a verifica complet că mesajul nu mai este eligibil pentru livrare normală, inspectează starea mesajelor pending ale brokerului sau folosește operația de status relevantă a proiectului, dacă există.

**Domeniu:** redrive-ul DLQ sau replay-ul manual **nu** sunt demonstrate de aceste dovezi.

---

## 3. Eșecul unui endpoint de broker și failover

**Obiectiv:** Verificarea faptului că un client configurat cu mai multe endpointuri de broker poate continua să funcționeze când unul dintre ele devine indisponibil.

### Configurație

Cele două procese broker au fost pornite pe:

- `localhost:50051`
- `localhost:50052`

Ambele loguri de pornire raportează aceeași cale de jurnal:

```text
gRPC broker listening on 50051; file journal: /tmp/messaging/scenario2.jsonl
gRPC broker listening on 50052; file journal: /tmp/messaging/scenario2.jsonl
```

Configurația intenționată a clientului folosește ambele endpointuri:

```bash
GRPC_ENDPOINTS=localhost:50051,localhost:50052
```

> Această variabilă de mediu trebuie să fie suportată de codul de configurare al aplicației pentru ca setarea să aibă efect.

### Dovezi observate

Consumatorul a întâmpinat o eroare `UNAVAILABLE`, a reîncercat conexiunea, a primit un mesaj, a întâmpinat o eroare de conexiune la ACK/NACK, a primit din nou același mesaj și a persistat în final ACK-ul.

Aceasta demonstrează recuperarea după o cădere de conexiune, dar logurile disponibile **nu** stabilesc că clientul a trecut cu succes de la un endpoint de broker la celălalt.

### Rezultat

**Status: verificat parțial; failover-ul între endpointuri nu este încă demonstrat.**

Pentru a finaliza scenariul:

1. Pornește ambele endpointuri de broker.
2. Confirmă că clientul este configurat să folosească ambele endpointuri.
3. Oprește un broker, lăsându-l pe celălalt în funcțiune.
4. Publică un mesaj sau reconectează consumatorul.
5. Verifică din logurile clientului și ale brokerului că endpointul rămas a procesat operația.
6. Confirmă că retry-urile folosesc același `messageId` logic și că operația nu este duplicată.

### Limitare: jurnal partajat

Ambele brokere folosesc `/tmp/messaging/scenario2.jsonl` ca jurnal. Acest lucru singur **nu** stabilește că accesul concurent al mai multor procese broker la fișier este sigur. Blocarea fișierului (file locking), scrierile concurente, comportamentul de recuperare și split-brain trebuie testate separat înainte de a afirma că jurnalul partajat oferă coordonare fiabilă între brokere.

---

## Concluzie

Dovezile disponibile demonstrează:

- reconectarea consumatorului;
- redelivery după un eșec de conexiune la ACK/NACK;
- confirmare (ACK) reușită;
- gestionarea DLQ după NACK-uri repetate.

Următoarele necesită dovezi suplimentare înainte de a fi considerate verificate:

- failover între endpointuri de broker;
- recuperare după restartul procesului broker;
- utilizarea concurentă sigură a unui jurnal partajat.

Aceste teste demonstrează comportament **at-least-once**, nu procesare **exactly-once**. Aplicațiile care consumă mesaje trebuie să țină cont de posibilele livrări duplicate.