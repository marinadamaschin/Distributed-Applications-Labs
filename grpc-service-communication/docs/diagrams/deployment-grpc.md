# Diagrama de deployment

```mermaid
flowchart TB
    subgraph Clients[Mașina / procesele client]
      P[Producer process]
      C[Consumer process]
      EP[GrpcEndpointPool]
      P --> EP
      C --> EP
    end
    subgraph Brokers[Instanțe broker gRPC]
      B1[Broker A :50051]
      B2[Broker B :50052]
    end
    subgraph Storage[Stocare persistentă]
      J1[(journal A / messages.jsonl)]
      J2[(journal B / messages.jsonl)]
      BK[(backup file)]
    end
    EP -->|gRPC / HTTP2| B1
    EP -->|gRPC / HTTP2| B2
    B1 --> J1
    B2 --> J2
    J1 -. backup / restore .-> BK
```

## Note de deployment

- Portul implicit al brokerului este `50051`; instanțele suplimentare trebuie configurate cu porturi diferite.
- Endpoint-urile trebuie configurate explicit în client.
- Un jurnal local per instanță **nu** oferă replicare automată între brokeri. Dacă instanțele folosesc stocare separată,
  failover-ul poate ajunge la o instanță care nu are aceeași stare. Pentru consistență multi-broker este necesar un
  jurnal comun cu locking fiabil sau un protocol de replicare distribuită.
- Nu expuneți porturile în rețele neîncredere fără TLS și autentificare.

```