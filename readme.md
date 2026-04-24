# Automated Data Warehouse Migration System

<p align="center">
  <img src="https://img.shields.io/badge/Airflow-2.9.1-brightgreen" alt="Airflow version">
  <img src="https://img.shields.io/badge/Greenplum-6.21.0-darkgreen" alt="Greenplum version">
  <img src="https://img.shields.io/badge/PostgreSQL-15-blue" alt="PostgreSQL version">
  <img src="https://img.shields.io/badge/Python-3.12-purple" alt="Python version">
  <img src="https://img.shields.io/badge/dbt-1.11.8-orange" alt="dbt 1.11.8">
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
migration-service/
├── README.md
├── docker-compose.yml
├── .env
├── .env.example
├── .gitignore
├── airflow/
│   └── dags/
│       ├── full_source_build_pipeline.py
│       ├── full_migration_orchestration_pipeline.py
│       ├── hr_pipeline.py
│       ├── risk_pipeline.py
│       ├── access_pipeline.py
│       ├── finance_pipeline.py
│       ├── hr_cloud_pipeline.py
│       ├── risk_cloud_pipeline.py
│       ├── access_cloud_pipeline.py
│       ├── finance_cloud_pipeline.py
│       ├── hr_dbt_pipeline.py
│       ├── risk_dbt_pipeline.py
│       ├── access_dbt_pipeline.py
│       └── finance_dbt_pipeline.py
├── bigquery/
│   ├── dds & dm build scripts
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── .user.yml
│   ├── macros/
│   │   ├── generate_schema_name.sql
│   │   └── type_casts.sql
│   ├── models/
│   │   ├── hr/
│   │   │   ├── dds/  
│   │   │   ├── dm/      
│   │   │   └── schema.yml
│   │   ├── risk/
│   │   │   ├── dds/      
│   │   │   ├── dm/       
│   │   │   └── schema.yml
│   │   ├── access/
│   │   │   ├── dds/      
│   │   │   ├── dm/       
│   │   │   └── schema.yml
│   │   └── finance/
│   │       ├── dds/      
│   │       ├── dm/       
│   │       └── schema.yml
├── infra/
│   ├── airflow/
│   │   ├── Dockerfile
│   │   └── requirements.txt
│   └── greenplum/
│       └── init/
│           ├── hr/
│           │   ├── sql-scripts
│           │   ├── metadata.yaml
│           │   └── dependencies.yaml
│           ├── risk/
│           │   ├── sql-scripts
│           │   ├── metadata.yaml
│           │   └── dependencies.yaml
│           ├── access/
│           │   ├── sql-scripts
│           │   ├── metadata.yaml
│           │   └── dependencies.yaml
│           └── finance/
│               ├── sql-scripts
│               ├── metadata.yaml
│               └── dependencies.yaml
├── scripts/
│   ├── init_greenplum_base.sh
│   ├── run_greenplum_sql.sh
│   ├── run_greenplum_query.sh
│   ├── cloud_export/
│   │   ├── hr/        
│   │   ├── risk/   
│   │   ├── access/ 
│   │   └── finance/
│   └── data_gen/
│       ├── generate_hr_raw_data.py
│       ├── generate_risk_raw_data.py
│       ├── generate_access_raw_data.py
│       ├── generate_finance_raw_data.py
│       └── output/
├── secrets/
│   └── gcp/
│       └── service-account.json
└── docs/
    ├── airflow_connection.png
    └── readme/
        ├── service-acc.png
        └── data-flow-scheme.png
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

### 5. Google Cloud Setup

* Create `service account`
* Create a JSON key for the service account and save it to: **secrets/gcp/service-account.json**

<p align="center">
  <img src="docs/service-acc.png" alt="svc-gc" width="900">
</p>

* Grant IAM roles: storage.objectAdmin, bigquery.dataEditor, bigquery.jobUser
* Create a Cloud Storage bucket
* Create BigQuery datasets - use the same region for all datasets