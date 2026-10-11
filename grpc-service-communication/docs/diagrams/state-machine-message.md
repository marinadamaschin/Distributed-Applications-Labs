# Mașina de stări a unui mesaj

```mermaid
stateDiagram-v2
    [*] --> Received: Publish RPC
    Received --> Persisted: jurnal scris durabil
    Persisted --> Pending: mesaj disponibil pentru livrare
    Pending --> InFlight: livrare către consumer
    InFlight --> Acknowledged: ACK
    InFlight --> RetryWait: NACK / timeout / eroare
    RetryWait --> Pending: încercări rămase
    RetryWait --> DeadLetter: încercări epuizate
    Acknowledged --> [*]
    DeadLetter --> [*]
```

`RetryWait` presupune un mecanism de întârziere/backoff. Dacă implementarea curentă nu persistă termenul următoarei
încercări sau nu reia efectiv retry-ul după restart, această diagramă reprezintă comportamentul țintă și trebuie
ajustată după testarea codului.

```