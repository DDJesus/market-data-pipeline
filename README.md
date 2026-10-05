# Market Data Pipeline

A hands-on data engineering lab built to practice the architecture, tooling, and failure modes involved in moving data from independent producers into a tested analytical warehouse.

The project uses synthetic market data rather than real financial data. The goal is not to produce trading insights or predictions; it is to build and understand a realistic data pipeline using tools such as Apache Airflow, PostgreSQL, dbt, Docker, Python, and SQL.

## Project Architecture

This repository is the consumer and transformation side of a three-repository lab:

- **Vendor A — `mock-market-lab`**
  - Synthetic market-data producer
  - Provides streaming and batch-oriented market data
  - Consumer integration is planned but not yet implemented in this repository

- **Vendor B — `company-sentiment`**
  - Independent synthetic company-sentiment producer
  - Publishes PREMARKET and POSTMARKET JSON batches
  - Currently integrated with this pipeline

- **`market-data-pipeline`**
  - Ingests vendor data
  - Preserves immutable raw artifacts
  - Loads structured data into PostgreSQL
  - Orchestrates scheduled work with Airflow
  - Transforms and tests warehouse data with dbt
    

<img width="4410" height="590" alt="mermaid-diagram" src="https://github.com/user-attachments/assets/41128e99-c2df-4316-b915-d8b5464915ff" />



## Why This Project Exists

This project was built as a practical data-engineering learning environment.

Instead of starting with a prepared dataset, the lab treats its synthetic vendors as independent external systems. Data crosses an HTTP boundary, is validated and preserved, loaded into a database, transformed into analytical models, and tested at multiple stages.

The intent is to practice not only the successful path, but also questions such as:

- What happens when a scheduled source is temporarily unavailable?
- Can an ingestion task safely run twice?
- What happens if an upstream artifact changes after it has already been ingested?
- How can the original source artifact be traced from warehouse data?
- Where should source-specific cleanup end and business transformations begin?
- How can data-quality assumptions be made executable rather than remaining documentation?
- How should identities shared across independent sources be resolved?

## Technologies

### Apache Airflow

Airflow 3 is used for orchestration.

The current Vendor B PREMARKET DAG:

- runs on a timezone-aware schedule
- executes shortly after the upstream publication time
- includes retry behavior for transient failures
- delegates ingestion logic to application code rather than embedding the implementation directly in the DAG

Airflow runs as separate API server, scheduler, and DAG processor services.

### PostgreSQL

PostgreSQL 17 provides persistent relational storage for both the local environment and the market-data warehouse.

Vendor B is represented using separate batch and observation grains:

```text
raw.vendor_b_batch
        │
        │ 1:N
        ▼
raw.vendor_b_observation
```

The batch table retains publication-level information and provenance while individual sentiment observations remain separately addressable.

### dbt

dbt Core is used for warehouse transformations and data-quality contracts.

The project currently demonstrates:

- declared `source()` tables
- `ref()` dependencies
- staging models
- intermediate models
- marts
- view and table materializations
- seeds
- generic data tests
- relationship tests
- uniqueness and nullability tests
- singular SQL tests
- dependency/DAG-based builds

Current transformation flow:

```text
raw.vendor_b_observation
            │
            ▼
stg_vendor_b__observations
            │
            ├──────── company_identity seed
            │                 │
            ▼                 ▼
int_vendor_b__sentiment_with_identity
            │
            ├──────── stg_vendor_b__batches
            │
            ▼
mart_company_sentiment_daily
```

The company identity seed explicitly maps Vendor B company names to market tickers. Identity resolution is intentionally kept out of raw ingestion.

### Python

Python handles source acquisition, validation, immutable landing, provenance generation, and warehouse loading.

Vendor payloads are validated before persistence.

Raw source bytes are preserved rather than parsing and reserializing the payload before landing.

### Docker Compose

Docker Compose provides the local execution environment for:

- PostgreSQL
- Airflow API server
- Airflow scheduler
- Airflow DAG processor

PostgreSQL data is stored in a persistent Docker volume so container recreation does not destroy warehouse state.

## Ingestion and Immutability

Vendor B artifacts are first stored as immutable raw files.

A simplified landing structure looks like:

```text
data/
└── raw/
    └── vendor_b/
        └── YYYY/
            └── MM/
                └── DD/
                    └── premarket/
                        ├── sentiment.json
                        └── metadata.json
```

The original response bytes are preserved in `sentiment.json`.

Metadata records information including:

- source URL
- retrieval timestamp
- HTTP status
- schema version
- market date
- batch type
- SHA-256 digest

Runtime raw data is intentionally excluded from Git.

### Idempotency

The ingestion path distinguishes between three cases:

```text
artifact does not exist
        → create it

artifact exists with identical SHA-256
        → unchanged / safe retry

artifact exists with different content
        → fail
```

The same principle is carried into the PostgreSQL loader.

A repeated load of the same logical batch does not duplicate it. If the logical batch already exists with different source content, the loader fails rather than silently overwriting warehouse history.

Warehouse loading is transactional, and the number of loaded observations is checked against the count declared by the source batch.

## Data Quality

Data-quality assumptions are tested explicitly with dbt.

Examples include:

- observation IDs must be unique and non-null
- batch IDs must resolve to an existing batch
- reference-data company mappings must be unique
- resolved market tickers must not be null
- mart grain must remain unique across its expected compound key

During development, the company identity mapping was deliberately broken to verify that unresolved companies remained visible in the transformation while the dbt quality test failed.

This was intentional: invalid reference data should produce a detectable failure rather than silently removing source observations through an inner join.

## Repository Layout

```text
market-data-pipeline/
├── dags/                  # Airflow DAG definitions
├── data/                  # Runtime landing area; raw data ignored by Git
├── dbt/
│   └── market_analytics/
│       ├── models/
│       │   ├── staging/
│       │   ├── intermediate/
│       │   └── marts/
│       ├── seeds/
│       └── tests/
├── src/
│   └── market_pipeline/   # Ingestion and warehouse application code
├── compose.yaml
└── .gitignore
```

## Current Scope

### Implemented

- Dockerized local PostgreSQL environment
- persistent database storage
- Airflow 3 orchestration environment
- Vendor B PREMARKET ingestion
- schema/date/batch validation
- immutable raw artifact landing
- SHA-256 provenance
- idempotent ingestion
- transactional PostgreSQL loading
- raw batch/observation relational model
- dbt staging layer
- dbt seed-based company identity resolution
- dbt intermediate layer
- aggregated sentiment mart
- dbt data-quality tests

### Planned

Vendor A integration is intentionally left as a future extension.

The planned design uses its two interfaces for different purposes:

```text
Vendor A SSE
    → long-running streaming consumer

Vendor A daily batch
    → scheduled batch ingestion / reconciliation
```

The SSE consumer would run as a long-lived service rather than as a permanently running Airflow task. Airflow would remain responsible for scheduled batch and reconciliation workflows.

The eventual goal is to reconcile streamed market events with the authoritative daily batch and combine Vendor A market identities with Vendor B sentiment data through the downstream modeling layer.

## Related Repositories

- `DDJesus/mock-market-lab` — synthetic Vendor A market-data producer
- `DDJesus/company-sentiment` — synthetic Vendor B company-sentiment producer

These producers are intentionally maintained independently from the consumer pipeline so that ingestion occurs across an external interface rather than by importing producer implementation code.

## Why These Technologies?

The stack was chosen to give each component a distinct responsibility rather than to maximize the number of technologies used.

**Apache Airflow — orchestration**

Airflow fits scheduled, dependency-driven workflows such as fetching a published batch, retrying transient failures, and coordinating downstream processing. It was not chosen as the eventual SSE runtime because a continuously running stream consumer has a different lifecycle from a scheduled DAG.

**PostgreSQL — relational warehouse and persistent state**

PostgreSQL provides durable relational storage, transactions, constraints, and familiar SQL semantics. The current data volume does not justify a distributed warehouse or processing platform, and PostgreSQL makes relational modeling behavior easy to inspect directly.

**dbt — transformation and data contracts**

Transformation logic is separated from ingestion. dbt provides explicit model dependencies, reusable SQL transformations, testing, documentation, and lineage while allowing the raw ingestion layer to remain source-oriented.

**Python — ingestion and boundary validation**

Python handles operations that are awkward or inappropriate to express as warehouse transformations: HTTP acquisition, byte-level artifact preservation, hashing, source-contract validation, and transactional loading.

**Docker Compose — reproducible local infrastructure**

Compose provides isolated, repeatable services without introducing orchestration infrastructure that the scale of this lab does not require. 

The project intentionally does not use technologies such as Spark or Kubernetes simply to demonstrate familiarity with them. At the current scale they would add operational complexity without solving a problem the lab actually has.

## Project Status

This is a learning and portfolio project, not a production trading system.

The current milestone demonstrates a complete Vendor B path from independently produced source data through orchestration, immutable landing, transactional warehouse loading, dbt transformation, and data-quality testing.

Future work will focus on adding Vendor A only where doing so introduces a new engineering problem—particularly streaming ingestion and batch reconciliation—rather than expanding the synthetic dataset simply for volume.
