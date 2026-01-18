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

## 8) Fault-tolerance demo

YouTube: https://youtu.be/5aNk8ygCM14

Below is the full demo script for the video. Commands are shown for **CMD**.

### (1) Network configuration

**A**
```cmd
ipconfig
```

**B**
```cmd
ipconfig
```

Explain:
- LAN IPs (Wi-Fi)
- both machines are in the same subnet

---

### (2) Start services

**A**
```cmd
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\run-cluster.ps1 -Mode cluster -Nodes "192.168.1.151,192.168.1.139" -Me 192.168.1.151 -Infra
```

**B**
```cmd
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\run-cluster.ps1 -Mode cluster -Nodes "192.168.1.151,192.168.1.139" -Me 192.168.1.139
```

---

### (3) Validate all services (local + remote)

**A**
```cmd
docker ps
curl -i "http://127.0.0.1:18080/status"
curl -i "http://127.0.0.1:7001/status"
curl -i "http://127.0.0.1:7002/index/status"
curl -i "http://127.0.0.1:7003/hz/members"

curl -i "http://192.168.1.139:7001/status"
curl -i "http://192.168.1.139:7002/index/status"
curl -i "http://192.168.1.139:7003/hz/members"
```

**B**
```cmd
docker ps
curl -i "http://127.0.0.1:7001/status"
curl -i "http://127.0.0.1:7002/index/status"
curl -i "http://127.0.0.1:7003/hz/members"

curl -i "http://192.168.1.151:18080/status"
curl -i "http://192.168.1.151:7001/status"
curl -i "http://192.168.1.151:7002/index/status"
curl -i "http://192.168.1.151:7003/hz/members"
```

---

### (4) Ingest + automatic indexing

**A**
```cmd
curl -s -X POST "http://127.0.0.1:7001/ingest/222"
curl -s "http://127.0.0.1:7002/index/status"
curl -i "http://127.0.0.1:18080/search?q=moon&limit=5"
```

**B**
```cmd
curl -s "http://127.0.0.1:7002/index/status"
curl -i "http://127.0.0.1:7003/search?q=moon&limit=5"
```

---

### (5) Search from both nodes

**A (LB)**
```cmd
curl -i "http://127.0.0.1:18080/search?q=the&limit=3"
curl -i "http://127.0.0.1:18080/search?q=human&limit=3"
curl -i "http://127.0.0.1:18080/search?q=war&limit=3"
```

**B (direct Search)**
```cmd
curl -i "http://127.0.0.1:7003/search?q=the&limit=3"
curl -i "http://127.0.0.1:7003/search?q=human&limit=3"
curl -i "http://127.0.0.1:7003/search?q=war&limit=3"
```

---

### (6) Kill Search on Node B (system keeps working)

**B**
```cmd
docker compose stop search
docker ps
curl -i "http://127.0.0.1:7003/hz/members"
curl -i "http://127.0.0.1:7003/search?q=moon&limit=5"
```

**A**
```cmd
curl -i "http://127.0.0.1:7003/hz/members"
curl -i "http://127.0.0.1:18080/search?q=moon&limit=5"
```

Explain:
- Hazelcast membership shrinks
- Nginx LB still forwards to the remaining Search replica

---

### (7) Restart Search on Node B (auto re-join)

**B**
```cmd
docker compose start search
docker ps
curl -i "http://127.0.0.1:7003/hz/members"
curl -i "http://127.0.0.1:7003/search?q=moon&limit=5"
```

**A**
```cmd
curl -i "http://127.0.0.1:7003/hz/members"
```

---

### (8) Stop Indexing on Node A (search still answers)

**A**
```cmd
docker compose stop indexing
docker ps
curl -i "http://127.0.0.1:7003/hz/members"
curl -i "http://127.0.0.1:18080/search?q=moon&limit=5"
```

**B**
```cmd
curl -i "http://127.0.0.1:7003/hz/members"
curl -i "http://127.0.0.1:7003/search?q=moon&limit=5"
```

---

### (9) Ingest while Indexing is down, then recover

**A (Indexing stopped)**
```cmd
curl -s -X POST "http://127.0.0.1:7001/ingest/34"
curl -i "http://127.0.0.1:7002/index/status"
curl -i "http://192.168.1.139:7002/index/status"
curl -i "http://127.0.0.1:18080/search?q=internet&limit=5"
```

**B**
```cmd
curl -i "http://127.0.0.1:7002/index/status"
curl -i "http://127.0.0.1:7003/search?q=internet&limit=5"
```

**A (recover Indexing)**
```cmd
docker compose start indexing
docker ps
curl -s "http://127.0.0.1:7002/index/status"
curl -i "http://127.0.0.1:18080/search?q=internet&limit=5"
```

---

### (10) Stop ingestion + stop indexing on the other node

**A**
```cmd
docker compose stop ingestion
docker ps
```

**B**
```cmd
docker compose stop indexing
docker ps
curl -i "http://127.0.0.1:7003/hz/members"

curl -s -X POST "http://127.0.0.1:7001/ingest/999"
curl -i "http://127.0.0.1:7003/search?q=commedia&limit=5"
```

**A (search via LB still works for existing index)**
```cmd
curl -i "http://127.0.0.1:18080/search?q=commedia&limit=5"

docker compose start ingestion
docker ps
```

**B (recover Indexing)**
```cmd
docker compose start indexing
docker ps
```

---

## 9) Notes

- **ActiveMQ UI:** `http://localhost:8161` (default credentials depend on image defaults)
- **Multi-machine run:** pass `--mq=tcp://<MQ_HOST>:61616` and `--hzMembers=<host1>,<host2>,...` (or use multicast/auto depending on the network)
- **Hazelcast replication:** index structures are configured with `backupCount=2` (Hazelcast caps backups to `clusterSize - 1` automatically)
- **Nginx dynamic discovery:** LB uses Docker DNS + `resolve` in the upstream so scaling `search` automatically updates available backends
- **LB config mount:** `infra/nginx/default.conf` is mounted as read-only (`:ro`) to keep the runtime config immutable
