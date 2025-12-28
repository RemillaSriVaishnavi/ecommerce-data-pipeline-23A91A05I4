# 🐳 Docker Deployment Guide  
## E-Commerce Data Pipeline

This document describes how to deploy and run the **E-Commerce Data Pipeline** using **Docker** and **Docker Compose**.  
It covers prerequisites, setup steps, service orchestration, data persistence, configuration, and troubleshooting.

---

## 1. Prerequisites

### 1.1 Software Requirements

| Component        | Minimum Version |
|------------------|-----------------|
| Docker           | 24.x or higher  |
| Docker Compose   | v2.20+          |
| Operating System | Windows / Linux / macOS |
| RAM              | Minimum 4 GB    |
| Disk Space       | Minimum 5 GB    |

### Verify Installation

```bash
docker --version
docker compose version
````

---

## 2. Docker Architecture Overview

The Docker deployment consists of **two services**:

| Service  | Description                                                                 |
| -------- | --------------------------------------------------------------------------- |
| postgres | PostgreSQL database containing staging, production, and warehouse schemas   |
| pipeline | Python-based data pipeline (ETL, data quality checks, analytics generation) |

### Service Communication

* Services communicate over a **Docker network**
* Database is accessed using the service name `postgres`
* No hardcoded IP addresses are used (Docker DNS-based resolution)

---

## 3. Quick Start Guide

### 3.1 Build Docker Images

From the project root directory:

```bash
docker compose build
```

---

### 3.2 Start Services

```bash
docker compose up -d
```

This will:

* Start PostgreSQL
* Wait for database health check
* Start the pipeline service

---

### 3.3 Verify Running Services

```bash
docker compose ps
```

**Expected output:**

* `postgres` → healthy
* `pipeline` → running / completed

---

### 3.4 Run Pipeline Inside Container

```bash
docker compose exec pipeline python scripts/pipeline_orchestrator.py
```

---

### 3.5 Access PostgreSQL Database

```bash
docker compose exec postgres psql -U admin -d ecommerce_db
```

**Available Schemas:**

* `staging`
* `production`
* `warehouse`

---

### 3.6 View Logs

**Pipeline logs:**

```bash
docker compose logs pipeline
```

**Database logs:**

```bash
docker compose logs postgres
```

---

### 3.7 Stop Services

```bash
docker compose down
```

---

### 3.8 Clean Up (Remove Volumes & Images)

```bash
docker compose down -v
docker system prune -f
```

---

## 4. Data Persistence

### 4.1 PostgreSQL Data Persistence

* PostgreSQL data is stored using **named Docker volumes**
* Data persists across container restarts

**Volume Mapping:**

```
postgres_data:/var/lib/postgresql/data
```

---

### 4.2 Pipeline Output Persistence

**Mounted Directories:**

* `data/`
* `logs/`

**Persisted Outputs:**

* Generated CSV files
* Analytics outputs
* Data quality reports
* Application logs

---

## 5. Configuration

### 5.1 Environment Variables

| Variable    | Description  |
| ----------- | ------------ |
| DB_HOST     | postgres     |
| DB_PORT     | 5432         |
| DB_NAME     | ecommerce_db |
| DB_USER     | admin        |
| DB_PASSWORD | admin        |

Defined in:

* `docker-compose.yml`
* `.env` file (optional)

---

### 5.2 Health Checks

**PostgreSQL Health Check:**

```bash
pg_isready -U admin -d ecommerce_db
```

* The pipeline service starts **only after** PostgreSQL is healthy

---

## 6. Troubleshooting

### Port Already in Use

```bash
lsof -i :5432
```

Change the exposed port in `docker-compose.yml` if required.

---

### Database Not Ready

```bash
docker compose logs postgres
```

Wait until the health check reports `healthy`.

---

### Pipeline Container Fails

```bash
docker compose logs pipeline
```

Check for:

* Python errors
* Missing dependencies
* Configuration issues

---

### Permission Issues (Linux)

```bash
sudo chown -R $USER:$USER data logs
```

---

### Network Connectivity Issues

```bash
docker network ls
docker network inspect ecommerce-network
```

---

## 7. Resource Limits (Optional)

Example resource constraints:

```yaml
deploy:
  resources:
    limits:
      cpus: "1.0"
      memory: 1024M
```

---

## 8. Best Practices Followed

✔ Named Docker volumes
✔ Health-based service dependencies
✔ No hardcoded IP addresses
✔ Clear service separation
✔ Logs and outputs persisted
✔ Docker Compose v2 syntax

---

## 9. Summary

This Docker setup ensures:

* Reliable service orchestration
* Safe and persistent data storage
* Easy local and CI deployment
* Production-grade project structure

The entire pipeline can be deployed and executed using a **single command**:

```bash
docker compose up -d
```

---

```



