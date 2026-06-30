# dbt-fabric-finance

A public reference implementation of a **bronze / silver / gold medallion architecture** on **Microsoft Fabric**, using **dbt** for SQL transformations and **Fabric Python notebooks** for data ingestion. The data domain is financial market data.

> **Note:** This repository contains the data platform only. The intelligence layer (Fabric IQ) lives in a separate repository and consumes the gold layer.

---

## Architecture

```
External APIs → Fabric Notebooks → Bronze Lakehouse → dbt (Silver) → dbt (Gold)
```

See [`docs/architecture.md`](docs/architecture.md) for the full diagram and layer responsibilities.

---

## Repository Layout

```
dbt-fabric-finance/
├── notebooks/          # Fabric Python ingestion notebooks (bronze landing)
│   ├── bronze/         # One notebook per data source
│   └── utils/          # Shared helper notebooks
├── models/             # dbt transformation models
│   ├── bronze/         # Thin views over raw Lakehouse tables + source definitions
│   ├── silver/         # Cleaned and conformed domain entities
│   └── gold/           # Business-ready aggregates and metrics
├── seeds/              # Static reference data (CSV)
├── snapshots/          # dbt snapshots for slowly changing dimensions
├── tests/              # Custom singular data tests
├── macros/             # Reusable Jinja/SQL macros
├── analyses/           # Ad-hoc analytical SQL (not materialised)
└── docs/               # Supplementary documentation
```

---

## Quickstart

### Prerequisites

- Python 3.11+
- [ODBC Driver 18 for SQL Server](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server)
- Access to a Microsoft Fabric workspace

### 1. Clone and install

```bash
git clone https://github.com/<your-org>/dbt-fabric-finance.git
cd dbt-fabric-finance
pip install -r requirements.txt
```

### 2. Configure credentials

```bash
cp .env.example .env
# Edit .env with your Fabric workspace and API values
```

Copy `profiles.yml.example` into `~/.dbt/profiles.yml` and fill in your Fabric connection details. **Do not put `profiles.yml` inside this repository.**

### 3. Install dbt packages

```bash
dbt deps
```

### 4. Verify the project parses

```bash
dbt compile
```

### 5. Run ingestion notebooks

Upload notebooks from `notebooks/bronze/` to your Fabric workspace and run them in order to land raw data into the bronze Lakehouse.

### 6. Run dbt transformations

```bash
dbt run
dbt test
```

---

## Contributing

- SQL is linted with SQLFluff (`sqlfluff lint models/`).
- All models must have YAML documentation and at least `not_null` + `unique` tests on primary keys.
- Notebook filenames follow `ingest_<source>_<entity>.ipynb`.
- dbt model names follow `<layer>__<entity>` (e.g. `silver__daily_prices`).

---

## License

MIT
