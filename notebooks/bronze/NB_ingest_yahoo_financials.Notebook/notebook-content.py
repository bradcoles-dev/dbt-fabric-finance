# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

%pip install yfinance --quiet

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

vl = notebookutils.variableLibrary.getLibrary("Variables")
WORKSPACE_ID = vl.WORKSPACE_ID
BRONZE_LAKEHOUSE_ID = vl.BRONZE_LAKEHOUSE_ID

LAKEHOUSE_SCHEMA = "dbo"
LAKEHOUSE_TABLE = "raw_financials"
WRITE_MODE = "overwrite"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import yfinance as yf
import pandas as pd
from datetime import datetime, timezone
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, DateType, TimestampType,
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

seeds_base = (
    f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com"
    f"/{BRONZE_LAKEHOUSE_ID}/Tables/{LAKEHOUSE_SCHEMA}"
)

sp100_df  = spark.read.format("delta").load(f"{seeds_base}/sp100_tickers").toPandas()
asx200_df = spark.read.format("delta").load(f"{seeds_base}/asx200_tickers").toPandas()

TICKERS = sp100_df["ticker"].tolist() + asx200_df["ticker"].tolist()
print(f"Universe: {len(sp100_df)} S&P 100 + {len(asx200_df)} ASX 200 = {len(TICKERS)} total tickers")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Stored in long/EAV format so variable metric availability across tickers
# and yfinance versions doesn't require schema changes. Silver models pivot
# to wide format for the specific metrics they need.

def statement_to_long(ticker: str, stmt_df: pd.DataFrame, stmt_type: str) -> pd.DataFrame:
    if stmt_df is None or stmt_df.empty:
        return pd.DataFrame()
    df = stmt_df.T.reset_index().rename(columns={"index": "period_end"})
    df = df.melt(id_vars="period_end", var_name="metric_name", value_name="value")
    df["ticker"] = ticker
    df["statement_type"] = stmt_type
    df["period_end"] = pd.to_datetime(df["period_end"], utc=True).dt.date
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    return df[["ticker", "period_end", "statement_type", "metric_name", "value"]]


def fetch_financials(ticker: str) -> pd.DataFrame:
    try:
        t = yf.Ticker(ticker)
        frames = []
        for stmt, label in [
            (t.financials,    "income_statement"),
            (t.balance_sheet, "balance_sheet"),
            (t.cashflow,      "cash_flow"),
        ]:
            df = statement_to_long(ticker, stmt, label)
            if not df.empty:
                frames.append(df)
        if not frames:
            print(f"WARNING: no financial data for {ticker}")
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)
    except Exception as e:
        print(f"ERROR fetching {ticker}: {e}")
        return pd.DataFrame()


frames = []
for ticker in TICKERS:
    print(f"Fetching {ticker}...")
    df = fetch_financials(ticker)
    if not df.empty:
        frames.append(df)

raw = pd.concat(frames, ignore_index=True)
raw["ingested_at"] = datetime.now(timezone.utc)
raw["source"] = "yahoo_finance"

print(f"\nFetched {len(raw):,} rows across {raw['ticker'].nunique()} tickers.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

schema = StructType([
    StructField("ticker",         StringType(),    False),
    StructField("period_end",     DateType(),      False),
    StructField("statement_type", StringType(),    False),
    StructField("metric_name",    StringType(),    False),
    StructField("value",          DoubleType(),    True),
    StructField("ingested_at",    TimestampType(), False),
    StructField("source",         StringType(),    False),
])

col_order = [f.name for f in schema.fields]
spark_df = spark.createDataFrame(raw[col_order], schema=schema)

table_path = (
    f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com"
    f"/{BRONZE_LAKEHOUSE_ID}/Tables/{LAKEHOUSE_SCHEMA}/{LAKEHOUSE_TABLE}"
)

(
    spark_df.write
    .format("delta")
    .mode(WRITE_MODE)
    .option("mergeSchema", "true")
    .save(table_path)
)

print(f"Written {len(raw):,} rows to {table_path}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f"Total rows      : {len(raw):,}")
print(f"Tickers         : {raw['ticker'].nunique()}")
print(f"Statement types : {sorted(raw['statement_type'].unique().tolist())}")
print(f"Unique metrics  : {raw['metric_name'].nunique()}")
print(f"Period range    : {raw['period_end'].min()} -> {raw['period_end'].max()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
