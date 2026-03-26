# Automated Data Warehouse Migration System

<p align="center">
  <img src="https://img.shields.io/badge/Airflow-2.9.1-brightgreen" alt="Airflow version">
  <img src="https://img.shields.io/badge/Greenplum-6.21.0-darkgreen" alt="Greenplum version">
  <img src="https://img.shields.io/badge/PostgreSQL-15-blue" alt="PostgreSQL version">
  <img src="https://img.shields.io/badge/Python-3.12-purple" alt="Python version">
  <img src="https://img.shields.io/badge/dbt-Core-orange" alt="dbt Core">
  <img src="https://img.shields.io/badge/BigQuery-Google%20Cloud-lightblue" alt="BigQuery">
</p>

## Description

This project is an automated system for migrating an on-premise analytical warehouse to Google Cloud.

The source system is **Greenplum** and the target platform is **BigQuery**. The idea is not just to move tables, but to build a controlled migration process with orchestration, cloud loading, analytical layer creation and validation.

---

## Architecture and Technologies

General flow:

**Greenplum → Apache Airflow → Google Cloud**

<p align="center">
  <img src="docs/readme/data-flow-scheme.png" alt="architecture-scheme" width="900">
</p>


* **Greenplum** — source analytical warehouse
* **Apache Airflow** — workflow orchestration
* **Google Cloud Storage** — intermediate cloud storage
* **BigQuery** — target analytical platform
* **dbt Core** — analytical layer modeling
* **Python** — migration service logic
* **PostgreSQL** — Airflow metadata database

Ports:

* **Airflow Webserver**: `8080`
* **Greenplum**: `5432`

---

## Project Structure

```text
data-migration-system/
├── README.md
├── docker-compose.yml
├── .env
├── .env.example
├── .gitignore
├── airflow/
│   └── dags/
├── docs/
│   └── readme/
├── infra/
│   ├── airflow/
│   │   ├── Dockerfile
│   │   └── requirements.txt
│   └── greenplum/
│       └── init/
│           └── init_greenplum.sh
├── src/
│   └── migration_service/
│       ├── connectors/
│       ├── extraction/
│       ├── loading/
│       ├── metadata/
│       └── validation/
├── dbt/
│   ├── dbt_project.yml
│   └── models/
├── metadata/
├── config/
└── scripts/
```

* **airflow/dags/** — Airflow DAGs
* **infra/airflow/** — Airflow image and dependencies
* **infra/greenplum/init/** — Greenplum initialization scripts
* **src/migration_service/** — main migration code
* **dbt/** — target analytical models
* **metadata/** — extracted schemas and artifacts
* **config/** — migration configuration
* **scripts/** — helper scripts

---

## Quick Start

### 1. Clone repository

```bash
git clone <repository-url>
cd data-migration-system
```

### 2. Create `.env` like .env.example


### 3. Start containers

```bash
docker compose up -d --build
```

### 4. Create Airflow Connection

Open `http://localhost:8080`

Login:

* **username**: `admin`
* **password**: `admin`

In Airflow UI go to:

**Admin → Connections → Add Connection**

Use these values:

* **Connection Id**: `greenplum_dwh`
* **Connection Type**: `Postgres`
* **Host**: `greenplum`
* **Schema**: `dwh`
* **Login**: `gpadmin`
* **Password**: `gpadmin`
* **Port**: `5432`
  
<p align="center">
  <img src="docs/airflow_connection.png" alt="architecture-scheme" width="900">
</p>