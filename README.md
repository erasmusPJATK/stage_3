# Big Data – Stage 3 Distributed Pipeline

This project is a distributed version of the Stage 2 book-processing pipeline.  
Stage 3 adds **ActiveMQ** and **Hazelcast** to turn the system into a small cluster that can run across multiple machines.

The system supports:

- **Distributed ingestion** of books (Project Gutenberg or local samples)
- **Asynchronous indexing** via a message broker (ActiveMQ)
- **Shared in-memory index** (Hazelcast) used by one or more search nodes
- Simple **status endpoints** to inspect the nodes

---

## 1. Architecture Overview

High-level data flow:

1. **Ingestion Service**
   - Fetches a book (from Project Gutenberg or local test files)
   - Stores it in a local **datalake** (header/body/meta files)
   - Publishes a message to **ActiveMQ** with the `book_id` (and optionally text/meta)

2. **Indexing Service**
   - Subscribes to the **ActiveMQ queue**
   - For each message:
     - Reads the book text (from the message or via HTTP/datalake)
     - Tokenizes and counts terms
     - Updates Hazelcast structures:
       - `docs` – document metadata
       - `doc-terms` – term frequencies by document
       - `inverted-index` – postings list (term → set of doc IDs)

3. **Search Service**
   - Joins the same **Hazelcast cluster**
   - Reads from `docs`, `doc-terms` and `inverted-index`
   - Exposes a `/search` endpoint that:
     - Parses `q` into terms
     - Uses TF–IDF-style scoring
     - Optionally filters by `author` and `language`
     - Returns top-k ranked documents

4. **Control Service**
   - Lightweight orchestrator from Stage 2
   - Can be extended in Stage 3 to coordinate multi-node workflows, but is not required for the basic distributed demo.

External components:

- **ActiveMQ broker** – message queue between ingestion and indexing
- **Hazelcast cluster** – embedded inside indexing/search nodes, used as shared index storage

We use **Option A: single-service nodes**: each process only runs one role (ingestion / indexing / search / control).

---

## 2. Modules

Repository layout (Stage 3 part):

- `ingestion-service/`
  - Javalin HTTP service
  - Local `datalake/` directory
  - Publishes messages to ActiveMQ

- `indexing-service/`
  - Javalin HTTP service
  - Subscribes to ActiveMQ
  - Writes to Hazelcast:
    - `docs`
    - `doc-terms`
    - `inverted-index`

- `search-service/`
  - Javalin HTTP service
  - Reads from Hazelcast and performs TF–IDF search

- `control-service/`
  - Javalin HTTP service
  - Optional coordinator (mainly Stage 2 functionality; can be reused/extended in Stage 3)

Each service is a standalone Maven module built into a **fat jar** with `maven-shade-plugin`.

---

## 3. Prerequisites

- Java **17+**
- Maven **3.x**
- Docker (for running ActiveMQ easily), or another way to run ActiveMQ
- Network connectivity between machines (for multi-node setup)

---

## 4. Building

From the project root:

```bash
mvn -q -DskipTests clean package


This produces shaded jars, for example:

* `ingestion-service/target/ingestion-service-3.0.0-all.jar`
* `indexing-service/target/indexing-service-3.0.0-all.jar`
* `search-service/target/search-service-3.0.0-all.jar`
* `control-service/target/control-service-3.0.0-all.jar`

(exact version may differ depending on the `pom.xml`).

---

## 5. External Services

### 5.1 ActiveMQ Broker

We use ActiveMQ as the message broker between ingestion and indexing nodes.

Example (Docker):

```bash
docker run --name mq --rm \
  -p 61616:61616 \
  -p 8161:8161 \
  apache/activemq-classic
```

* Broker URL used in the services: `tcp://<MQ_HOST>:61616`

### 5.2 Hazelcast Cluster

Hazelcast is embedded inside **indexing-service** and **search-service**.
All nodes must be configured with:

* the same `cluster` name (e.g. `bd-stage3`), and
* the same `--hz=<HZ_MEMBER_HOST>:5701` (initial contact point).

No external Hazelcast process is required; indexing and search nodes join the cluster automatically.

---

## 6. Running the Services (Generic)

The services are parameterized so they can run either all on one machine, or across multiple nodes.

Placeholders used below:

* `<MQ_HOST>` – host where ActiveMQ is running
* `<INGESTION_HOST>` – host where ingestion-service runs
* `<INDEXING_HOST>` – host where indexing-service runs
* `<SEARCH_HOST>` – host where search-service runs
* `<CONTROL_HOST>` – host where control-service runs (optional)
* `<HZ_MEMBER_HOST>` – host used as Hazelcast contact point

### 6.1 Ingestion Service

```bash
java -jar ingestion-service/target/ingestion-service-3.0.0-all.jar \
  --mq=tcp://<MQ_HOST>:61616 \
  --origin=http://<INGESTION_HOST>:7001 \
  --port=7001
```

### 6.2 Indexing Service

```bash
java -jar indexing-service/target/indexing-service-3.0.0-all.jar \
  --mq=tcp://<MQ_HOST>:61616 \
  --hz=<HZ_MEMBER_HOST>:5701 \
  --hzPort=5701 \
  --port=7002
```

You can start multiple indexing nodes (on the same or different machines) with different `--port` values, but sharing the same `--mq` and Hazelcast settings.

### 6.3 Search Service

```bash
java -jar search-service/target/search-service-3.0.0-all.jar \
  --hz=<HZ_MEMBER_HOST>:5701 \
  --hzPort=5701 \
  --port=7003
```

You can also run several search nodes on different ports/hosts, as long as they join the same Hazelcast cluster.

### 6.4 Control Service (optional)

```bash
java -jar control-service/target/control-service-3.0.0-all.jar \
  --port=7000
```

---

## 7. HTTP API Overview

### 7.1 Ingestion Service

Base URL: `http://<INGESTION_HOST>:<port>` (default port `7001`)

* `POST /ingest/{book_id}`
  Fetches a Project Gutenberg book by ID, stores it in the local datalake, and publishes an indexing event to MQ.

* `GET /ingest/status/{book_id}`
  Returns datalake status for the book (`header`/`body` presence, etc.).

* `GET /ingest/list`
  Lists all known book IDs in the datalake.

* `GET /ingest/file/{book_id}/body`
  Returns raw text body for a given book ID from the local datalake
  (used in Stage 3 for simple distributed tests with local sample documents).

* `GET /status`
  Returns basic info and configuration such as:

  ```json
  {
    "service": "ingestion",
    "origin": "http://<INGESTION_HOST>:7001",
    "mq": "tcp://<MQ_HOST>:61616",
    "port": 7001
  }
  ```

### 7.2 Indexing Service

Base URL: `http://<INDEXING_HOST>:<port>` (default `7002`)

* `POST /index/update/{book_id}`
  Forces re-index of a single book from the datalake (Stage 2-style API; still useful for debugging).

* `POST /index/rebuild`
  Rebuilds the entire index based on all books in the datalake (Stage 2 functionality).

* `GET /index/status`
  Returns current index status (books indexed, index size, version, etc.).

* `GET /status`
  Stage 3 status endpoint, e.g.:

  ```json
  {
    "service": "indexing",
    "port": 7002,
    "mq": "tcp://<MQ_HOST>:61616",
    "hzMembers": "<HZ_MEMBER_HOST>:5701",
    "docs": 1,
    "terms": 8
  }
  ```

Internally, the indexing service:

* Listens to an ActiveMQ queue for ingestion events.
* Tokenizes text and computes term frequencies.
* Updates Hazelcast data structures:

  * `docs` – metadata for each `book_id` (title, author, language, etc.)
  * `doc-terms` – map from `book_id` to `{term → tf}`
  * `inverted-index` – multimap from `term` to set of `book_id`s

### 7.3 Search Service

Base URL: `http://<SEARCH_HOST>:<port>` (default `7003`)

* `GET /search`
  Query parameters:

  * `q` – free text query (space-separated terms)
  * `author` (optional) – filter by author (exact match, case-insensitive)
  * `language` (optional) – filter by language
  * `k` (optional) – max number of results (default: 10)

  Response structure:

  ```json
  {
    "query": "<normalized-query>",
    "filters": {
      "author": "...",
      "language": "..."
    },
    "count": <number-of-results>,
    "results": [
      {
        "book_id": 111,
        "title": "Sample 111",
        "author": "Unknown",
        "language": "English",
        "score": 0.6931471805599453
      }
    ]
  }
  ```

* `GET /hz/stats`
  Basic Hazelcast stats seen by this node:

  ```json
  {
    "docs": 1,
    "terms": 8
  }
  ```

* `GET /hz/members`
  Returns the list of Hazelcast cluster members that this node sees.

* `GET /status`
  Status of the search service:

  ```json
  {
    "service": "search",
    "port": 7003,
    "hzMembers": "<HZ_MEMBER_HOST>:5701"
  }
  ```

### 7.4 Control Service (optional)

Base URL: `http://<CONTROL_HOST>:<port>` (default `7000`)

* `GET /status`
  Returns simple JSON, for example:

  ```json
  {
    "service": "control",
    "status": "running"
  }
  ```

* `POST /control/run/{book_id}`
  (Stage 2-style orchestration)
  Previously triggered ingestion and indexing in sequence.
  In Stage 3, this can be extended to coordinate distributed nodes, but the minimal distributed pipeline does not require it.

---

## 8. Notes / Next Steps

* Current design uses **logical replication**:

  * Raw text (full book body) lives on ingestion nodes’ local `datalake/`.
  * Processed index data (`docs`, `doc-terms`, `inverted-index`) is shared across the cluster via Hazelcast.
* This avoids heavyweight file replication between heterogeneous machines, while still providing cluster-wide search.

Possible future improvements:

* Multiple indexing nodes consuming from the same ActiveMQ queue for natural load balancing.
* Multiple search nodes behind a reverse proxy or simple round-robin client.
* Extending control-service to drive distributed ingestion/indexing/search workflows end-to-end.

