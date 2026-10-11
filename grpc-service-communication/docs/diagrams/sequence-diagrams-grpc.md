# Diagrame de secvență — implementarea gRPC

Aceste diagrame documentează fluxurile principale ale implementării. Ele descriu comportamentul arhitectural
intenționat; pentru predare, confruntați fiecare pas cu testele și comportamentul executat efectiv.

## 1. Publicarea și persistența mesajului

```mermaid
sequenceDiagram
    autonumber
    participant P as Producer
    participant LB as GrpcEndpointPool
    participant B as gRPC Broker
    participant S as FileMessageStore
    P->>LB: Publish(messageId, topic, payload)
    LB->>B: Publish RPC (protobuf/HTTP2)
    B->>S: verifică messageId și persistă evenimentul
    alt mesaj nou
        S-->>B: scriere reușită
        B-->>LB: PublishReply(accepted)
        LB-->>P: răspuns
    else ID deja folosit cu același conținut
        S-->>B: rezultat deduplicat
        B-->>LB: PublishReply(accepted/duplicate)
        LB-->>P: răspuns idempotent
    else ID existent cu alt conținut
        S-->>B: conflict
        B-->>LB: status de eroare
        LB-->>P: eroare
    end
```

## 2. Consum, ACK/NACK, retry și DLQ

```mermaid
sequenceDiagram
    autonumber
    participant C as Consumer
    participant B as gRPC Broker
    participant S as FileMessageStore
    C->>B: Subscribe(topic) [server-streaming]
    B->>S: citește mesaje eligibile neconfirmate
    S-->>B: mesaj
    B-->>C: Message
    alt procesare reușită
        C->>B: Ack(messageId)
        B->>S: persistă ACK
        S-->>B: confirmare scriere
        B-->>C: AckReply(success)
    else procesare nereușită
        C->>B: Nack(messageId)
        B->>S: persistă încercarea/evenimentul de respingere
        alt mai sunt încercări disponibile
            B->>B: așteaptă backoff exponențial
            B->>S: reia mesajul pentru livrare
        else limită de 3 încercări atinsă
            B->>S: persistă transferul în DLQ
        end
        B-->>C: NackReply(accepted)
    end
```

## 3. Recuperare după repornirea brokerului

```mermaid
sequenceDiagram
    autonumber
    participant O as Operator / Runtime
    participant B as gRPC Broker
    participant S as FileMessageStore
    O->>B: pornește procesul
    B->>S: deschide jurnalul JSONL
    S-->>B: evenimente persistate
    B->>B: reconstruiește mesajele, ACK-urile, abonamentele și DLQ
    B->>S: identifică mesajele neconfirmate eligibile
    S-->>B: stare recuperată
    B-->>O: serverul gRPC devine disponibil
```

## 4. Load balancing și failover la apelul RPC

```mermaid
sequenceDiagram
    autonumber
    participant P as Producer / Client
    participant LP as GrpcEndpointPool
    participant A as Broker A
    participant B as Broker B
    P->>LP: Publish(request)
    LP->>LP: selectează endpoint round-robin
    LP->>A: RPC
    alt Broker A răspunde
        A-->>LP: răspuns
        LP-->>P: rezultat
    else eroare de conectare/RPC eligibilă pentru failover
        A--xLP: eroare / timeout
        LP->>B: încearcă endpoint-ul următor
        alt Broker B răspunde
            B-->>LP: răspuns
            LP-->>P: rezultat
        else toate endpoint-urile eșuează
            B--xLP: eroare
            LP-->>P: eroare finală
        end
    end
```

**Limită:** failover-ul după un timeout poate repeta o cerere a cărei rezultat nu a ajuns la client. Deduplicarea după
ID reduce riscul pentru publicare, dar nu transformă sistemul într-o livrare exactly-once. Brokerii trebuie să
folosească jurnalul comun și blocări fiabile dacă împart aceeași stare.

## 5. Backup și restaurare

```mermaid
sequenceDiagram
    autonumber
    participant T as Scheduler / Operator
    participant S as FileMessageStore
    participant F as Fișier jurnal
    participant BK as Fișier backup
    T->>S: declanșează backup
    S->>F: sincronizează / citește starea jurnalului
    S->>BK: scrie copia de siguranță
    BK-->>S: backup finalizat
    T-->>S: (la restaurare) solicită restaurarea din backup
    S->>BK: citește și validează backup-ul
    S->>F: reconstruiește/înlocuiește starea conform procedurii documentate
    S-->>T: rezultat restaurare
```

Backup-ul trebuie făcut într-un mod consistent cu scrierile concurente. Testați restaurarea pe o copie separată a
directorului de date, nu peste singura copie a jurnalului.

## 6. Backup și restaurare

```mermaid
sequenceDiagram
    participant Operator
    participant Broker
    participant Store as FileMessageStore
    participant Backup as Backup file
    Operator->>Broker: solicită backup (mecanism configurat)
    Broker->>Store: citește starea/jurnalul
    Store->>Backup: scrie snapshot
    Backup-->>Operator: fișier backup disponibil
    Note over Operator,Backup: La restaurare, brokerul se oprește, se validează backup-ul și se restaurează fișierul înainte de repornire.
```

## 7. Rutare și abonare

```mermaid
sequenceDiagram
    participant C as Consumer
    participant S as gRPC service
    participant R as Subscription/routing state
    C->>S: Subscribe(consumer_id, topic)
    S->>R: înregistrează abonarea
    R-->>S: abonare acceptată
    S-->>C: flux de mesaje
    C->>S: Unsubscribe(consumer_id, topic)
    S->>R: dezactivează abonarea
    R-->>S: stare actualizată
    S-->>C: rezultat
```

Aceste secvențe sunt modele de interacțiune. Verificați ca fiecare operație menționată să existe în contractul
`messaging.proto` și în implementare înainte de a o prezenta drept funcționalitate demonstrată.