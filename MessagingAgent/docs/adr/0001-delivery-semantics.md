# ADR 0001 - Alegerea semanticii de livrare
## Context
Sistemul tratează explicit mesajele neconfirmate și crash-ul Consumer-ului după efectul local, dar înainte de ACK.
## Decizie
Semantica este **at-least-once** cu retry limitat. ACK confirmă procesarea. La NACK sau lipsă ACK, Brokerul poate retrimite mesajul. Sunt permise maximum 3 încercări, apoi mesajul ajunge în DLQ. Consumer-ul deduplică după `messageId`.
## Alternative
**At-most-once** ar putea pierde mesajul la cădere. `Exactly-once` necesită mecanisme tranzacționale suplimentare și nu rezultă doar din ACK + deduplicare.
## Consecințe
Pot exista livrări duplicate; deduplicarea este necesară. FIFO este per destinație, nu global.
## Limită cunoscută
Nu există `exactly-once`; există o fereastră între efectul local și persistarea marcajului de deduplicare. Cozile Brokerului sunt tranzitorii/in-memory.
