# Decizii tehnice și răspuns la feedback

## 1. DW independente și sincronizare

**Problema inițială:** Main pornea două DW în aceeași JVM cu aceeași instanță EmployeeStore.

**Modificare:** Main pornește un singur rol. DW1 și DW2 sunt procese independente, fiecare cu propriul RemoteEmployeeStore. Ambele folosesc serviciul de stocare, care deține singura copie autoritativă și sincronizează accesul concurent. Nu există replici locale ce ar trebui reconciliate.

**Motivație:** am ales alternativa unui serviciu de sincronizare, pentru a evita instalarea Cassandra și dependențele suplimentare într-o demonstrație locală. Datele și propunerile se păstrează într-un snapshot persistent. Serviciul este o implementare didactică de repository, nu un înlocuitor general pentru o bază de date.

**Compromis:** DW-urile pot cădea separat; stocarea rămâne un punct unic de indisponibilitate. Pentru disponibilitate ridicată ar trebui folosită o bază de date replicată sau un serviciu de stocare replicat cu un protocol de consens. Nu se pretinde că acestea sunt implementate.

**Persistență:** în PersistentEmployeeStore, fiecare scriere produce o copie nouă a stării. Snapshot-ul temporar este sincronizat pe disc și mutat atomic. Starea în memorie este înlocuită numai după succesul salvării. Dacă un proces moare după înlocuirea fișierului, dar înaintea răspunsului HTTP, modificarea poate fi deja aplicată: clientul trebuie să verifice rezultatul. Un lock de fișier împiedică două procese să dețină același repository. Fișiere temporare rămase după SIGKILL nu sunt încărcate ca stare validă.

Teste: `test_round_robin_and_independent_processes`, `test_shared_data_and_persistence_after_storage_restart`, `test_post_pull_confirmation_and_persistent_proposal`.

## 2. Cache fără lacăt în apelul de rețea

**Problema inițială:** ReentrantReadWriteLock era deținut pe toată durata apelului HTTP și bloca inclusiv verificarea cache-ului.

**Modificare:** ProxyController are un monitor `cacheState` folosit numai pentru câteva operații în memorie. Nici `client.send`, nici trimiterea răspunsului către client nu se execută în acest monitor.

La începutul unei scrieri se incrementează `activeWrites` și `generation` și se golește cache-ul. În `finally`, după succes sau eșec, se decrementează `activeWrites`, se incrementează din nou generația și se golește cache-ul. O citire memorează generația de început. Publică rezultatul în cache numai dacă generația este încă aceeași și nu există scrieri active. Verificarea și inserarea sunt în aceeași secțiune critică scurtă.

**De ce nu doar un singur increment:** o citire începută în timpul unei scrieri ar putea memora generația deja incrementată, primi date vechi și termina înaintea scrierii. Contorul de scrieri active și invalidarea la final acoperă și această situație, inclusiv scrieri suprapuse.

O citire concomitentă poate primi versiunea veche, dar nu o păstrează pentru cereri ulterioare. În timpul scrierii cache-ul este invalidat; citirile merg la DW fără să aștepte lacătul unui apel de rețea. Serviciul de stocare serializează operațiile efective de repository pentru consistență.

Teste: `test_cache_miss_hit_expiry_and_put_invalidation`, `test_slow_write_does_not_block_reads_or_cache_stale_result`, `test_old_read_finishing_after_write_cannot_repopulate_cache`.

## 3. Health checks, rotație și failover

ProxyController verifică `/health` la pornire și periodic prin ScheduledExecutorService. WarehouseController verifică disponibilitatea serviciului de stocare. Numai DW-urile sănătoase intră în lista Round Robin. O eroare de transport sau 5xx marchează imediat nodul ca indisponibil; un health check ulterior cu 200 îl reintroduce.

GET este reîncercat pe alt nod sănătos. PUT/POST au o singură încercare la nivelul aplicației: o scriere poate fi aplicată chiar dacă răspunsul se pierde. Nu se garantează zero erori pentru scrieri în fereastra dintre ultima verificare și căderea nodului. Când căderea este detectată, scrierile noi sunt trimise nodului rămas.

Testele opresc forțat un DW, verifică GET și PUT prin supraviețuitor, repornesc DW-ul și verifică reintegrarea. Un alt test oprește ambele DW-uri și verifică 503.

Teste: `test_failover_reads_writes_and_node_recovery`, `test_all_nodes_down`, `test_round_robin_and_independent_processes`.

## 4. Smart Proxy și conexiuni

Registrul inFlight fără utilizare a fost eliminat. În soluția revizuită, Smart Proxy înseamnă un intermediar care păstrează contextul sesiunii clientului și îl folosește independent de nodul DW ales. Metoda `session` din ProxyController:

1. Citește cookie-ul PADSESSION.
2. Regăsește sesiunea sau creează un UUID nou; nu acceptă drept sesiune nouă un ID arbitrar propus de client.
3. Actualizează ultima activitate și numărul de cereri.
4. Returnează cookie-ul, X-Session-Id și X-Session-Requests; contextul este folosit în logarea fiecărei cereri, inclusiv HIT.

Sesiunea poate continua peste conexiuni TCP diferite și peste alternarea DW-urilor. Nu înseamnă sticky sessions, autentificare sau stocare durabilă în Redis. Limitele sunt maximum 1000 de sesiuni, 30 de minute de inactivitate și pierderea sesiunilor la restartul proxy-ului.

Conexiunile persistente sunt o funcție separată: HttpServer permite HTTP/1.1 keep-alive către client; un HttpClient comun gestionează conexiunile către DW. Testul verifică explicit reutilizarea socket-ului client–proxy. Nu pretindem că testul inspectează conexiunile TCP interne proxy–DW.

Test: `test_correlation_sessions_and_keepalive`.

## 5. X-Correlation-Id

HttpUtil.correlation păstrează antetul primit dacă este valid; generează UUID dacă lipsește sau este gol. Validarea limitează ID-ul la 128 de caractere ASCII din alfabetul documentat în mesajul de eroare, pentru a evita injectarea de linii în loguri. Un ID nevalid produce 400.

Proxy-ul îl transmite către DW și îl întoarce clientului. WarehouseController scrie `DW`, `correlation`, metoda, URI-ul și statusul în log. Cererile de health check au ID-uri proprii cu prefixul `health-`. La HIT nu există apel DW, dar ID-ul cererii curente apare în răspuns și în logul proxy-ului.

Test: `test_correlation_sessions_and_keepalive` caută ID-ul furnizat de client în logurile DW.

## 6. Statusuri HTTP și teste

Repository-ul decide atomic dacă PUT creează sau înlocuiește. Crearea produce 201 și Location; înlocuirea produce 200. Proxy-ul păstrează Location din răspunsul DW.

HttpUtil.negotiate negociază application/json și application/xml, cu wildcard-uri și ponderi q. Dacă niciuna dintre reprezentări nu este acceptabilă, proxy-ul și DW returnează 406 înaintea modificării datelor. Nu se face fallback implicit la JSON pentru Accept: text/csv.

Testele automate sunt în Python unittest și pornesc JVM-uri reale. Alegerea evită dependența de JUnit sau de descărcări Maven. `bash scripts/test.sh` compilează și rulează totul; `mvn test` nu este comanda acestei suite.

Teste: `test_formats_and_validation`, `test_cache_miss_hit_expiry_and_put_invalidation`; raportul complet este în `rezultate-teste.txt`.

## Formulare pentru prezentarea orală

„Am separat proxy-ul, cele două DW-uri și serviciul de stocare în patru procese. DW-urile citesc și modifică aceeași stare persistentă printr-un serviciu care sincronizează operațiile. Am eliminat lacătul ținut în timpul apelurilor HTTP; cache-ul folosește generații și evidența scrierilor active, astfel încât o citire veche să nu fie memorată după o modificare. Proxy-ul verifică periodic sănătatea nodurilor, scoate nodul căzut din rotație și îl reintroduce când revine. Păstrează sesiunea clientului prin cookie și propagă correlation ID în logurile DW. Am adăugat teste automate pentru cache, alternare, failover, persistență, sesiuni și concurență, plus statusurile 201 și 406.”
