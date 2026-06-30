# Notebooks

Fabric Python notebooks that ingest raw financial market data and land it in the **bronze Lakehouse**.

## Structure

| Folder | Purpose |
|--------|---------|
| `bronze/` | One notebook per data source. Each notebook pulls from an external API or file and writes a Delta table to the bronze Lakehouse schema. |
| `utils/` | Shared helper notebooks (authentication, logging, schema utilities) referenced by `%run` from bronze notebooks. |

## Conventions

- Notebook filenames follow `ingest_<source>_<entity>.ipynb` (e.g. `ingest_polygon_daily_prices.ipynb`).
- Each bronze notebook must write to a table named `raw_<entity>` in the `bronze` schema.
- Notebooks are exported from Fabric as `.ipynb` files and committed here for version control.
