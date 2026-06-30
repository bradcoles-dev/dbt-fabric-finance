# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# PARAMETERS CELL ********************

TICKERS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "META",
    "TSLA", "NVDA", "JPM", "GS", "BRK-B",
]
START_DATE = "2015-01-01"
END_DATE = None           # None = today
LAKEHOUSE_TABLE = "raw_daily_prices"
WRITE_MODE = "overwrite"  # "overwrite" for full reload, "append" for incremental

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%pip install yfinance --quiet

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import yfinance as yf
import pandas as pd
from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, LongType, DateType, TimestampType,
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def fetch_ticker(ticker: str, start: str, end) -> pd.DataFrame:
    try:
        t = yf.Ticker(ticker)
        hist = t.history(start=start, end=end, auto_adjust=True, actions=False)
        if hist.empty:
            print(f"WARNING: no data returned for {ticker}")
            return pd.DataFrame()
        hist = hist.reset_index()
        hist.columns = [c.lower().replace(" ", "_") for c in hist.columns]
        hist["ticker"] = ticker
        return hist
    except Exception as e:
        print(f"ERROR fetching {ticker}: {e}")
        return pd.DataFrame()


frames = []
for ticker in TICKERS:
    print(f"Fetching {ticker}...")
    frame = fetch_ticker(ticker, START_DATE, END_DATE)
    if not frame.empty:
        frames.append(frame)

raw = pd.concat(frames, ignore_index=True)
print(f"\nFetched {len(raw):,} rows across {raw['ticker'].nunique()} tickers.")
raw.head()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

raw["date_"] = pd.to_datetime(raw["date"]).dt.date
raw["ingested_at"] = datetime.now(timezone.utc)
raw["source"] = "yahoo_finance"

raw = raw.drop(columns=["date"])
raw = raw.rename(columns={
    "open":   "open_",
    "high":   "high_",
    "low":    "low_",
    "close":  "close_",
    "volume": "volume_",
})

# volume can arrive as float when NaNs are present
raw["volume_"] = raw["volume_"].fillna(0).astype("int64")

print(raw.dtypes)
print(f"Date range: {raw['date_'].min()} -> {raw['date_'].max()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

schema = StructType([
    StructField("ticker",      StringType(),    False),
    StructField("date_",       DateType(),      False),
    StructField("open_",       DoubleType(),    True),
    StructField("high_",       DoubleType(),    True),
    StructField("low_",        DoubleType(),    True),
    StructField("close_",      DoubleType(),    True),
    StructField("volume_",     LongType(),      True),
    StructField("ingested_at", TimestampType(), False),
    StructField("source",      StringType(),    False),
])

spark_df = spark.createDataFrame(raw, schema=schema)

(
    spark_df.write
    .format("delta")
    .mode(WRITE_MODE)
    .partitionBy("date_")
    .option("mergeSchema", "true")
    .saveAsTable(LAKEHOUSE_TABLE)
)

print(f"Written {spark_df.count():,} rows to '{LAKEHOUSE_TABLE}'.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

result = spark.table(LAKEHOUSE_TABLE)
date_range = result.agg(
    F.min("date_").alias("min_date"),
    F.max("date_").alias("max_date"),
).collect()[0]
tickers_loaded = [
    r.ticker for r in result.select("ticker").distinct().orderBy("ticker").collect()
]

print(f"Total rows : {result.count():,}")
print(f"Tickers    : {tickers_loaded}")
print(f"Date range : {date_range.min_date} -> {date_range.max_date}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
