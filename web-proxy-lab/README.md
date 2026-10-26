# PAD - Laboratorul 3: Web Proxy

Java 17, Maven pentru import IntelliJ; aplicația nu are biblioteci externe.

## Integrarea în proiectul tău

1. Copiază folderul **web-proxy-lab** în **Distributed-Applications-Labs**, la același nivel cu **grpc-service-communication** și **MessagingAgent**.
2. În IntelliJ, click dreapta pe `web-proxy-lab/pom.xml` → **Add as Maven Project** (sau Maven → Link Maven Projects și selectează acest pom).
3. Selectează JDK 17 sau mai nou în Project Structure → Modules → web-proxy-lab → Dependencies.
4. Deschide `src/main/java/md/utm/pad/Main.java` și apasă Run lângă `main`.

Nu înlocui pom-urile celorlalte laboratoare. Acest folder este un modul Maven independent; nu necesită un pom părinte. Cele trei laboratoare vor apărea în același proiect IntelliJ.

Alternativ, în terminal, din folderul web-proxy-lab:

```bash
bash scripts/run.sh
```

Oprește cu Ctrl+C sau butonul Stop din IntelliJ. Porturile 8080, 8081 și 8082 trebuie să fie libere.

## Componente și cerințe

| Cerință | Implementare |
|---|---|
| HTTP concurent, controller | WarehouseController, câte un pool de 32 fire pentru fiecare server; o cerere activă este procesată de un fir |
| Colecții protejate | EmployeeStore cu metode synchronized și snapshot-uri imutabile |
| GET și PUT | Consultare și publicare angajați, inclusiv paginare |
| POST și pull | Propuneri de modificare cu sequence; GET /updates/employees?after=N; sursa confirmă prin PUT |
| JSON / XML | Accept: application/json sau application/xml; implicit JSON |
| Reverse proxy | ProxyController, adresă client http://localhost:8080 |
| Cache | GET /employees, TTL 5 secunde, maximum aproximativ 1000 intrări; cheia include query și Accept |
| Invalidare | După scrieri; lock citire/scriere împiedică reintroducerea unui răspuns vechi în timpul unei scrieri prin proxy |
| Round Robin | Contor atomic, alternare între DW 8081 și 8082 la cererile care ajung la backend |
| Smart Proxy | Corelare cerere–răspuns prin UUID și registru inFlight, curățat la final; conexiuni HTTP persistente către clienți și HttpClient reutilizat către DW |
| Erori | 400, 404, 405, 413, 415; timeout și 502 la indisponibilitatea DW; retry doar la GET |

## Demonstrație

Deschide `requests.http` în IntelliJ HTTP Client, dacă ediția ta îl include. Alternativ:

```bash
curl -i http://localhost:8080/employees
curl -i http://localhost:8080/employees
curl -i -H 'Accept: application/xml' http://localhost:8080/employees
curl -i -H 'Cache-Control: no-cache' http://localhost:8080/employees
curl -i -H 'Cache-Control: no-cache' http://localhost:8080/employees
curl -i -X PUT -d 'id=4&name=Alexandra' http://localhost:8080/employees
curl -i http://localhost:8080/employees
curl -i -X POST -d 'id=4&name=Alexandra%20Manea' http://localhost:8080/employees
curl -i 'http://localhost:8080/updates/employees?after=0'
curl -i -X PUT -d 'id=4&name=Alexandra%20Manea' http://localhost:8080/employees
```

Repetă primele două cereri în maximum 5 secunde: X-Cache trece de la MISS la HIT. După expirare revine MISS. Cu no-cache observi alternarea X-Warehouse. Fiecare răspuns proxy are un X-Correlation-Id nou, inclusiv răspunsurile din cache.

Concurență, în terminal macOS:

```bash
seq 1 20 | xargs -P 8 -I '{}' curl -s -H 'Cache-Control: no-cache' http://localhost:8080/employees
```

## Alegeri și limite explicite

Cele două noduri DW sunt servere HTTP distincte în același proces JVM și folosesc aceeași instanță EmployeeStore. Acesta este un model local pentru laborator, cu stocare în memorie: datele și propunerile se pierd la restart. Nodurile nu pot fi distribuite pe calculatoare diferite fără înlocuirea repository-ului cu o bază de date comună. Cassandra și Redis sunt recomandări în enunț și nu sunt incluse în această variantă.

Clienții folosesc exclusiv portul 8080. Porturile DW sunt accesibile local pentru etapa 1; scrierile directe în DW ocolesc invalidarea cache-ului, iar datele pot rămâne vechi până la TTL. Toate serverele ascultă doar pe loopback.

Scrierile folosesc application/x-www-form-urlencoded; alegerea JSON/XML se aplică datelor returnate, conform enunțului. Schema simplă: id pozitiv, name de 1–100 caractere. POST înregistrează o propunere; nu schimbă datele confirmate. GET updates nu consumă propunerile, iar sursa urmărește cursorul after și publică versiunea acceptată prin PUT. Rolul sursei este demonstrat prin requests.http/curl; nu există un serviciu separat de sincronizare.

Cache-ul aparține acestui proxy unic. Registrul Smart Proxy reține contextul cererilor active în memorie și nu este o sesiune persistentă în Redis. HttpClient gestionează reutilizarea conexiunilor; identitatea unui client nu este legată de un DW anume. Nu există autentificare: proiect pentru demonstrație locală.
