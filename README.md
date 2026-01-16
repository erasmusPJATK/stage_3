# Big Data – Stage 3 (Cluster Architecture)
Distributed Search Engine Project (ULPGC)

Stage 3 extends Stage 2 into a **distributed and fault-tolerant cluster** using:
- **ActiveMQ** for event-driven ingestion → indexing
- **Hazelcast** for a distributed, replicated in-memory index
- **Nginx** as a load balancer in front of horizontally-scaled Search

---

## 1) High-level Architecture

**Asynchronous flow:**
1. **Ingestion Service** downloads a Project Gutenberg book, stores it in a local datalake and publishes an event to **ActiveMQ**.
2. **Indexing Service** consumes events from ActiveMQ and updates the **Hazelcast** replicated in-memory index.
3. **Search Service** queries Hazelcast and returns ranked results (TF–IDF-like scoring).
4. **Load Balancer (Nginx)** distributes requests across multiple Search replicas.

```
Client
  │
  ▼
Nginx LB (:18080)
  │
  ├──► Search replica #1 (:7003)
  ├──► Search replica #2 (:7003)
  └──► Search replica #3 (:7003)
            │
            ▼
     Hazelcast Cluster
            ▲
            │
Indexing Service (:7002)
            ▲
            │
       ActiveMQ Queue
            ▲
            │
Ingestion Service (:7001)
```

---

## 2) Services and Ports

| Service | Purpose | Port(s) |
|---|---|---|
| `ingestion-service` | downloads + parses + stores datalake + publishes events | `7001` |
| `indexing-service` | consumes MQ events, builds replicated index | `7002` |
| `search-service` | reads Hazelcast index, serves search API | internal `7003` (via LB) |
| `search-lb` (nginx) | load balances search replicas | `18080` |
| `activemq` | message broker + UI | `61616`, `8161` |

Optional (not required by Stage 3):
- `control-service` (`7000`) – helper orchestrator for demo

---

## 3) Persistence: Datalake Storage

The ingestion datalake is mounted at:
- **inside container:** `/app/datalake`
- **backed by a Docker named volume:** `ingestion_datalake`

This means the datalake is **decoupled from containers**:
- `docker compose down` → containers removed, **datalake stays**
- `docker compose down -v` → containers removed, **datalake volume is deleted** (data reset)

### Inspect datalake (from the ingestion container)
```bash
docker exec -it ingestion sh -c "ls -lah /app/datalake && find /app/datalake -maxdepth 3 -type d"
```

---

## 4) Run with Docker Compose

> Windows users: use CMD/PowerShell and the commands below as-is.

### 4.1 Start the full stack (core + LB)
```bash
docker compose -f docker-compose.infra.yml -f docker-compose.yml up -d --build
```

### 4.2 Scale Search horizontally
```bash
docker compose -f docker-compose.infra.yml -f docker-compose.yml up -d --scale search=3
```

### 4.3 Stop (safe, keeps datalake)
```bash
docker compose -f docker-compose.infra.yml -f docker-compose.yml down
```

### 4.4 Full reset (WARNING: deletes datalake)
```bash
docker compose -f docker-compose.infra.yml -f docker-compose.yml down -v
```

---

## 5) Quick E2E Test Commands

### 5.1 Smoke test (all services)
```bash
curl http://localhost:7001/status
curl http://localhost:7002/status
curl http://localhost:18080/status
```

### 5.2 Ingest sample books
```bash
curl -X POST http://localhost:7001/ingest/84
curl -X POST http://localhost:7001/ingest/1342
curl -X POST http://localhost:7001/ingest/1661
```

### 5.3 Check indexing progress
```bash
curl http://localhost:7002/index/status
```

### 5.4 Search via load balancer
```bash
curl "http://localhost:18080/search?q=love&limit=5"
curl "http://localhost:18080/search?q=monster&limit=5"
```

### 5.5 Filters
```bash
curl "http://localhost:18080/search?q=love&author=austen&limit=10"
curl "http://localhost:18080/search?q=love&language=English&limit=10"
curl "http://localhost:18080/search?q=love&year=1813&limit=10"
```

---

## 6) High Availability Demo (Failover)

1) Run with 3 Search replicas:
```bash
docker compose -f docker-compose.infra.yml -f docker-compose.yml up -d --scale search=3
```

2) Stop one replica:
```bash
docker stop task3-search-2
```

3) Search must still work:
```bash
curl "http://localhost:18080/search?q=love&limit=5"
```

4) Start it again:
```bash
docker start task3-search-2
```

---

## 7) Benchmarks (Load & Scaling)

Recommended tool: **hey**

Example run:
```bash
hey -n 2000 -c 50 "http://localhost:18080/search?q=love&limit=10"
```

Scalability procedure:
1) Run with `search=1` and record throughput + latency.
2) Scale to `search=2` and repeat.
3) Scale to `search=3` and repeat.

Commands:
```bash
# search = 1
docker compose -f docker-compose.infra.yml -f docker-compose.yml up -d --scale search=1
hey -n 2000 -c 50 "http://localhost:18080/search?q=love&limit=10"

# search = 2
docker compose -f docker-compose.infra.yml -f docker-compose.yml up -d --scale search=2
hey -n 2000 -c 50 "http://localhost:18080/search?q=love&limit=10"

# search = 3
docker compose -f docker-compose.infra.yml -f docker-compose.yml up -d --scale search=3
hey -n 2000 -c 50 "http://localhost:18080/search?q=love&limit=10"
```

Tip: before measuring, do a small warm-up:
```bash
curl "http://localhost:18080/search?q=love&limit=10" > NUL
```

---

## 8) Demo Video



---

## 9) Notes

- **ActiveMQ UI:** `http://localhost:8161` (default credentials depend on image defaults)
- **Multi-machine run:** pass `--mq=tcp://<MQ_HOST>:61616` and `--hzMembers=<host1>,<host2>,...` (or use multicast/auto depending on the network)
- **Hazelcast replication:** index structures are configured with `backupCount=2` (Hazelcast caps backups to `clusterSize - 1` automatically)
- **Nginx dynamic discovery:** LB uses Docker DNS + `resolve` in the upstream so scaling `search` automatically updates available backends
- **LB config mount:** `infra/nginx/default.conf` is mounted as read-only (`:ro`) to keep the runtime config immutable
