# Testare automată gRPC

## Rulare

Necesită JDK 17+ și Maven 3.9+.

Linux/macOS:

```bash
./scripts/test-all.sh
```

Windows PowerShell:

```powershell
.\scripts\test-all.ps1
```

Comanda Maven execută `clean test`; pluginul Protobuf generează codul Java din `src/main/proto/messaging.proto` înainte
de compilare.

## Acoperire

| Domeniu                   | Teste                                                                        |
|---------------------------|------------------------------------------------------------------------------|
| Persistență / restart     | `publishAndAckSurviveRestart`, `subscriptionAndUnsubscriptionSurviveRestart` |
| Idempotency / deduplicare | publicare identică acceptată, ID cu alt conținut respins                     |
| ACK                       | ACK cunoscut, ACK necunoscut, ACK repetat și stare după restart              |
| Retry / DLQ               | epuizarea celor trei încercări și persistența DLQ                            |
| Izolare                   | topic-uri diferite și ACK separat per consumator                             |
| Backup / restore          | backup, modificare ulterioară și restaurare                                  |
| Formatul jurnalului       | evenimente JSONL publish/subscribe/ack                                       |
| gRPC / HTTP/2             | teste locale reale pentru publish, status, validare, duplicate și ACK        |
| Intrări invalide          | ID gol și backup inexistent                                                  |

## Limitări ale verificării

Testele din surse trebuie executate pe mediul de predare. În mediul în care a fost pregătită arhiva, comanda `mvn test`
nu a putut fi executată deoarece Maven nu era instalat; prin urmare nu se afirmă că suita trece. Testele automate nu
înlocuiesc testele operaționale multi-proces pentru concurență pe sistemul de fișiere comun, întreruperea procesului în
timpul scrierii, indisponibilitatea brokerilor și verificarea failover-ului real.

## Scenarii manuale de eșec

1. Publică un mesaj și oprește brokerul; repornește-l cu același fișier jurnal și verifică `GetStatus`.
2. Publică același ID cu același conținut de două ori; numărul mesajelor trebuie să rămână 1.
3. Publică același ID cu payload diferit; RPC trebuie să întoarcă `ALREADY_EXISTS`.
4. NACK de trei ori și verifică apariția în DLQ.
5. Configurează două endpoint-uri gRPC și oprește primul; verifică failover-ul clientului. Pentru stare coerentă, ambele
   instanțe trebuie să utilizeze stocare comună cu blocări de fișier fiabile.
6. Creează backup, publică alt mesaj, restaurează backup-ul și verifică starea restaurată.