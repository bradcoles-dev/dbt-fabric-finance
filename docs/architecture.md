# Architecture

## Overview

This repository implements a **medallion lakehouse architecture** for financial market data on **Microsoft Fabric**, with **dbt** handling all SQL transformations.

```
External APIs / Files
        │
        ▼
┌───────────────────┐
│  Fabric Notebooks │  (notebooks/bronze/)
│  Python ingestion │
└────────┬──────────┘
         │ Delta tables
         ▼
┌───────────────────┐
│   Bronze Layer    │  Raw, unmodified source data
│   (Lakehouse)     │  Schema: bronze
└────────┬──────────┘
         │ dbt models/bronze/
         ▼
┌───────────────────┐
│   Silver Layer    │  Cleaned, typed, conformed entities
│   (Lakehouse)     │  Schema: silver
└────────┬──────────┘
         │ dbt models/silver/
         ▼
┌───────────────────┐
│    Gold Layer     │  Business-ready aggregates and metrics
│   (Lakehouse)     │  Schema: gold
└───────────────────┘
         │
         ▼
  Intelligence Layer
  (Fabric IQ — separate repo)
```

## Layer Responsibilities

### Bronze
- Written by Fabric Python notebooks, not dbt.
- One Delta table per source entity, named `raw_<entity>`.
- No transformations — data is landed exactly as received.
- dbt `source()` definitions point here; bronze dbt models are thin views only.

### Silver
- Owned entirely by dbt.
- Applies cleaning, type casting, deduplication, and surrogate key generation.
- Conforms data into canonical domain entities (instruments, prices, corporate actions, etc.).
- Materialised as Delta tables.

### Gold
- Owned entirely by dbt.
- Produces business-ready aggregates, metrics, and analytical marts.
- Optimised for consumption by reporting tools and the intelligence layer.
- Materialised as Delta tables.

## Key Conventions

- dbt model names follow `<layer>__<entity>` (e.g. `silver__daily_prices`).
- Source YAML files use the prefix `_sources.yml`; model YAML files use `_<layer>__models.yml`.
- Notebook filenames follow `ingest_<source>_<entity>.ipynb`.
- Credentials are never stored in this repository — see `.env.example` and `profiles.yml.example`.
