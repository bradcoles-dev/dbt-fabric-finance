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
LAKEHOUSE_TABLE = "raw_company_info"
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
    StringType, DoubleType, LongType, TimestampType,
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

INFO_FIELDS = {
    "shortName":        "company_name",
    "sector":           "sector",
    "industry":         "industry",
    "country":          "country",
    "currency":         "currency",
    "quoteType":        "quote_type",
    "marketCap":        "market_cap",
    "beta":             "beta",
    "trailingPE":       "pe_ratio_ttm",
    "forwardPE":        "pe_ratio_forward",
    "priceToBook":      "price_to_book",
    "dividendYield":    "dividend_yield",
    "trailingEps":      "eps_ttm",
    "returnOnEquity":   "return_on_equity",
    "returnOnAssets":   "return_on_assets",
    "debtToEquity":     "debt_to_equity",
    "currentRatio":     "current_ratio",
    "operatingMargins": "operating_margin",
    "profitMargins":    "profit_margin",
    "grossMargins":     "gross_margin",
    "revenueGrowth":    "revenue_growth_yoy",
    "earningsGrowth":   "earnings_growth_yoy",
}


def fetch_info(ticker: str) -> dict:
    try:
        info = yf.Ticker(ticker).info
        row = {"ticker": ticker}
        for src_key, dst_key in INFO_FIELDS.items():
            row[dst_key] = info.get(src_key)
        return row
    except Exception as e:
        print(f"ERROR fetching {ticker}: {e}")
        return {"ticker": ticker, **{v: None for v in INFO_FIELDS.values()}}


rows = []
for ticker in TICKERS:
    print(f"Fetching {ticker}...")
    rows.append(fetch_info(ticker))

raw = pd.DataFrame(rows)
raw["ingested_at"] = datetime.now(timezone.utc)
raw["source"] = "yahoo_finance"

print(f"\nFetched info for {len(raw)} tickers.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# market_cap arrives as float from yfinance; cast to int for LongType
raw["market_cap"] = raw["market_cap"].apply(lambda x: int(x) if pd.notna(x) else None)

float_cols = [
    "beta", "pe_ratio_ttm", "pe_ratio_forward", "price_to_book",
    "dividend_yield", "eps_ttm", "return_on_equity", "return_on_assets",
    "debt_to_equity", "current_ratio", "operating_margin", "profit_margin",
    "gross_margin", "revenue_growth_yoy", "earnings_growth_yoy",
]
for col in float_cols:
    raw[col] = pd.to_numeric(raw[col], errors="coerce")

schema = StructType([
    StructField("ticker",              StringType(),    False),
    StructField("company_name",        StringType(),    True),
    StructField("sector",              StringType(),    True),
    StructField("industry",            StringType(),    True),
    StructField("country",             StringType(),    True),
    StructField("currency",            StringType(),    True),
    StructField("quote_type",          StringType(),    True),
    StructField("market_cap",          LongType(),      True),
    StructField("beta",                DoubleType(),    True),
    StructField("pe_ratio_ttm",        DoubleType(),    True),
    StructField("pe_ratio_forward",    DoubleType(),    True),
    StructField("price_to_book",       DoubleType(),    True),
    StructField("dividend_yield",      DoubleType(),    True),
    StructField("eps_ttm",             DoubleType(),    True),
    StructField("return_on_equity",    DoubleType(),    True),
    StructField("return_on_assets",    DoubleType(),    True),
    StructField("debt_to_equity",      DoubleType(),    True),
    StructField("current_ratio",       DoubleType(),    True),
    StructField("operating_margin",    DoubleType(),    True),
    StructField("profit_margin",       DoubleType(),    True),
    StructField("gross_margin",        DoubleType(),    True),
    StructField("revenue_growth_yoy",  DoubleType(),    True),
    StructField("earnings_growth_yoy", DoubleType(),    True),
    StructField("ingested_at",         TimestampType(), False),
    StructField("source",              StringType(),    False),
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

print(f"Written {len(raw)} rows to {table_path}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f"Tickers with P/E     : {raw['pe_ratio_ttm'].notna().sum()} / {len(raw)}")
print(f"Tickers with P/B     : {raw['price_to_book'].notna().sum()} / {len(raw)}")
print(f"Tickers with yield   : {raw['dividend_yield'].notna().sum()} / {len(raw)}")
print(f"Tickers with ROE     : {raw['return_on_equity'].notna().sum()} / {len(raw)}")
print(f"Tickers with mkt cap : {raw['market_cap'].notna().sum()} / {len(raw)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
