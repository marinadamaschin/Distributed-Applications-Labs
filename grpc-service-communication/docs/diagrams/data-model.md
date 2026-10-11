# Model logic al datelor persistate

```mermaid
erDiagram
    MESSAGE ||--o{ DELIVERY : has
    SUBSCRIPTION ||--o{ DELIVERY : receives
    MESSAGE ||--o| DEAD_LETTER : may_become
    MESSAGE {
      string message_id
      string topic
      string payload
      string created_at
    }
    SUBSCRIPTION {
      string consumer_id
      string topic
      string status
    }
    DELIVERY {
      string message_id
      string consumer_id
      int attempt_count
      string delivery_status
      string acked_at
    }
    DEAD_LETTER {
      string message_id
      string reason
      int attempts
      string failed_at
    }
```

Aceasta este schema logică pentru explicarea proiectului. Formatul fizic exact depinde de evenimentele serializate
efectiv de `FileMessageStore` în jurnalul JSONL.

```