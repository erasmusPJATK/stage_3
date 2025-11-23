# Big Data — Stage 2

Monorepo with four independent services (SOA) that implement the pipeline **Ingestion → Indexing → Search**, with **Control** orchestrating the flow.

- Course spec/hints: [Big_Data\_\_\_Stage_2_Project_Hints.pdf](/mnt/data/Big_Data___Stage_2_Project_Hints.pdf)
- JDK: 17
- Build tool: Maven
- Default ports: `7000` (control), `7001` (ingestion), `7002` (indexing), `7003` (search)

---

## Architecture (high level)

```
+-----------+         +-----------+         +-----------+
|  CONTROL  |  --->   | INGESTION |  --->   | INDEXING  |
|  :7000    |         | :7001     |         | :7002     |
+-----------+         +-----------+         +-----------+
       \                                         |
        \                                        v
         \---------------------------------->  SEARCH
                                               :7003
```

- **Ingestion**: downloads Project Gutenberg plain-text books, splits header/body, extracts metadata (`title`, `author`, `language`, `year`), stores into the **datalake**.
- **Indexing**: builds per-document JSON (`docs/{id}.json`) with TF, plus sharded **inverted index** (`inverted/*.json`) in **datamarts**.
- **Search**: query processing with TF-IDF scoring over the inverted index; filters by `author`, `language`, `year`.
- **Control**: orchestrates the end-to-end run for a given `book_id` and **notifies Search** to refresh (`POST /refresh`) after indexing.

---

## Repository layout (short)

```
ingestion-service/
  ├─ src/main/java/org/ulpgc/bd/ingestion/...
  ├─ datalake/
  │   └─ YYYYMMDD/HH/
  │       ├─ {id}_header.txt
  │       ├─ {id}_body.txt
  │       └─ {id}_meta.json
  └─ target/

indexing-service/
  ├─ src/main/java/org/ulpgc/bd/indexing/...
  ├─ datamarts/
  │   ├─ docs/{id}.json
  │   ├─ inverted/{a|b|...|num|other}.json
  │   ├─ index_status.json
  │   └─ indexing.log
  └─ target/

search-service/
  ├─ src/main/java/org/ulpgc/bd/search/...
  └─ target/

control-service/
  ├─ src/main/java/org/ulpgc/bd/control/...
  └─ target/
```

---

## Stage 2 compliance (what’s implemented)

- **Ingestion (7001)**: `POST /ingest/{id}`, `GET /ingest/status/{id}`, `GET /ingest/list`
  Stores `{id}_header.txt`, `{id}_body.txt`, `{id}_meta.json` (`title`, `author`, `language`, `year`). Logs ingestion events.
- **Indexing (7002)**: `POST /index/update/{id}`, `POST /index/rebuild`, `GET /index/status`
  Produces `datamarts/docs/{id}.json` (meta + TF) and `datamarts/inverted/*.json` (sharded inverted index).
- **Search (7003)**: `GET /search` with TF-IDF ranking; filters `author`, `language`, `year`; `POST /refresh` is available for cache/refresh notification.
- **Control (7000)**: `POST /control/run/{id}` orchestrates ingestion → (wait) → indexing → `POST /search/refresh`.

---

## Quick start (Windows)

> Note: Some modules produce a fat-jar (`*-all.jar`) via the Shade plugin. If you don’t have a fat-jar for a module, use **Variant B** (exec:java) below.

### 1) Build (CMD)

```bat
cd ingestion-service && mvn -q -DskipTests clean package && cd ..
cd indexing-service  && mvn -q -DskipTests clean package && cd ..
cd search-service    && mvn -q -DskipTests clean package && cd ..
cd control-service   && mvn -q -DskipTests clean package && cd ..
```

### 2) Run — Variant A (fat-jar, CMD)

```bat
start "ingestion" java -jar ingestion-service\target\ingestion-service-1.0.0-all.jar
start "indexing"  java -jar indexing-service\target\indexing-service-1.0.0-all.jar
start "search"    java -jar search-service\target\search-service-1.0.0-all.jar
start "control"   java -jar control-service\target\control-service-1.0.0-all.jar
```

### 2) Run — Variant A (PowerShell)

```powershell
Start-Process -NoNewWindow -FilePath java -ArgumentList '-jar','ingestion-service\target\ingestion-service-1.0.0-all.jar'
Start-Process -NoNewWindow -FilePath java -ArgumentList '-jar','indexing-service\target\indexing-service-1.0.0-all.jar'
Start-Process -NoNewWindow -FilePath java -ArgumentList '-jar','search-service\target\search-service-1.0.0-all.jar'
Start-Process -NoNewWindow -FilePath java -ArgumentList '-jar','control-service\target\control-service-1.0.0-all.jar'
```

### 2) Run — Variant B (exec:java, when no `-all.jar`)

```bat
cd ingestion-service && mvn -q exec:java -Dexec.mainClass=org.ulpgc.bd.ingestion.IngestionServiceApp
cd indexing-service  && mvn -q exec:java -Dexec.mainClass=org.ulpgc.bd.indexing.IndexingServiceApp
cd search-service    && mvn -q exec:java -Dexec.mainClass=org.ulpgc.bd.search.SearchServiceApp
cd control-service   && mvn -q exec:java -Dexec.mainClass=org.ulpgc.bd.control.ControlService
```

---

## APIs

### Ingestion — `http://localhost:7001`

- `POST /ingest/{id}` — fetch + persist book into the datalake
- `GET /ingest/status/{id}` — check presence of body/header for the book
- `GET /ingest/list` — list of ingested ids

### Indexing — `http://localhost:7002`

- `POST /index/update/{book_id}` — index a single book
- `POST /index/rebuild` — rebuild all indices from the datalake
- `GET /index/status` — global index stats

### Search — `http://localhost:7003`

- `GET /status` — health
- `POST /refresh` — control notification to refresh
- `GET /search` — query with filters

  - Query params:

    - `q` — free-text (tokenized)
    - `author` — exact match filter
    - `language` — exact match filter
    - `year` — integer filter (e.g., `1913`)
    - `k` — max results (default 10)

### Control — `http://localhost:7000`

- `GET /status` — health
- `POST /control/run/{book_id}` — run end-to-end: ingest → wait → index → notify search

---

## Manual tests (Windows CMD)

### 1) Orchestrated pipeline (Control)

```bat
curl -X POST "http://localhost:7000/control/run/111"
```

### 2) Ingestion only

```bat
curl -X POST "http://localhost:7001/ingest/111"
curl "http://localhost:7001/ingest/status/111"
curl "http://localhost:7001/ingest/list"
```

### 3) Indexing

```bat
curl -X POST "http://localhost:7002/index/update/111"
curl -X POST "http://localhost:7002/index/rebuild"
curl "http://localhost:7002/index/status"
```

### 4) Search

```bat
curl "http://localhost:7003/status"

:: full-text (TF-IDF)
curl "http://localhost:7003/search?q=alice&k=10"

:: filters
curl "http://localhost:7003/search?q=freckles&author=Gene%20Stratton-Porter&k=5"
curl "http://localhost:7003/search?q=philosophy&language=English&k=5"
curl "http://localhost:7003/search?q=bassington&year=1913&k=5"

:: manual refresh (usually triggered by Control)
curl -X POST "http://localhost:7003/refresh"
```

---

## Data formats

### Datalake (under `ingestion-service/datalake/YYYYMMDD/HH/`)

- `{id}_header.txt`

  ```
  Title: ...
  Author: ...
  Language: ...
  Year: 1913
  ```

- `{id}_body.txt` — book content (START/END PG blocks removed).
- `{id}_meta.json` — metadata + download info (checksum, parser_version, timings).

### Datamarts (under `indexing-service/datamarts/`)

- `docs/{id}.json`

  ```json
  {
    "book_id": 111,
    "title": "FRECKLES",
    "author": "Gene Stratton-Porter",
    "language": "English",
    "year": 1904,
    "terms": { "freckles": 123, "limberlost": 45, "...": 1 }
  }
  ```

- `inverted/a.json` (example shard)

  ```json
  {
    "alice": { "11": 37, "999": 2 },
    "adventure": { "11": 12 }
  }
  ```

- `index_status.json` — index stats (doc count, size MB, ts, version).

---

## Implementation notes

- **Split heuristics**: detect PG START/END, trim them from body.
- **Metadata extraction**: robust regex + heuristics for `Title/Author/Language` and **Year** (prefer contextual hints like “Release Date”, fall back to first reasonable 4-digit year).
- **Inverted index**: shards by first character (`a`–`z`, `num`, `other`) for simple file-based scalability.
- **Ranking**: TF-IDF with `idf = ln(1 + N/df)`, cumulative score per query term.
- **Control → Search**: `POST /refresh` after indexing (as per Stage 2 “notify search” requirement).

---

## FAQ / Troubleshooting

- **Port already in use**: change port or kill the process (`netstat -ano | findstr :7001` → `taskkill /PID <pid> /F`).
- **No `-all.jar`**: use **Variant B** (`mvn exec:java`) or add the Maven Shade Plugin to that module.
- **No search results**: verify that ingestion produced `*_header.txt` + `*_body.txt`, indexing generated `docs/{id}.json` and that your query terms exist in the relevant shard; also check your `year` filter matches the header/meta.
- **Accented characters**: tokenizer normalizes (NFD → strip accents), removes punctuation, ignores short English stop-words.

---

## Handy cheatsheet

```bat
:: Build all
mvn -q -DskipTests clean package

:: Run (fat-jar)
java -jar ingestion-service\target\ingestion-service-1.0.0-all.jar
java -jar indexing-service\target\indexing-service-1.0.0-all.jar
java -jar search-service\target\search-service-1.0.0-all.jar
java -jar control-service\target\control-service-1.0.0-all.jar

:: Full pipeline
curl -X POST "http://localhost:7000/control/run/111"

:: Search with year filter
curl "http://localhost:7003/search?q=alice&year=1865&k=5"
```

---

## Licenses / Sources

- Texts: Project Gutenberg (Public Domain, jurisdiction-dependent).
- Code: course project sources (clarify in Stage 3 if a license file is required).
