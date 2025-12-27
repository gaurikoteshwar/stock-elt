# stock-elt
Build a reliable ELT pipeline for historical and daily NVIDIA stock price data, enabling analytics-ready KPIs and scenario analysis.

This project builds a reliable ELT pipeline to ingest historical and daily stock price data, store it in PostgreSQL as a raw source of truth, and establish a strong foundation for analytics-ready KPI tables and scenario-based analysis.

The primary focus is on correctness, idempotency, and maintainable data modeling rather than downstream analytics or visualisation.

High-Level Architecture

yfinance API -> Python (Extract + Load) -> PostgreSQL (raw.stock_prices)

Data Model (Raw Layer)
Schema: raw
Table: stock_prices

Column	Description
symbol - Stock ticker (e.g. AAPL, MSFT)
date - Trading date
open - Opening price
high - Highest price of the day
low - Lowest price of the day
close - Closing price
volume - Trading volume
ingestion_ts - Timestamp when the record was ingested

Design principles:
- Append-only table
- Primary key: (symbol, date)
- No transformations or business logic applied
- Serves as the single source of truth for downstream models

How to Run
Setup and execution instructions will be added once the ingestion pipeline is implemented.

Roadmap
Phase 2: dbt staging models and business KPI tables 
Phase 3: Risk metrics, Monte Carlo simulations, and scenario analysis 
Phase 4: Visualisation and reporting layer 

-----------------------------------------------------------------------------------------------------------------------------------------------------

Phase 1: Focused exclusively on data ingestion and raw data reliability - DONE
- Ingest historical and daily stock price data from the yfinance API
- Load raw, append-only stock price data into PostgreSQL
- Support idempotent daily updates (safe to re-run without duplication)
- Define and enforce a stable raw data contract

Phase 2: Built staging table stg_stock_prices from raw stock data - DONE
- Created KPI table stock_kpis_daily with metrics: daily return, 20-day rolling volatility, rolling Sharpe, max drawdown, OBV.
- Validated data: row counts, date ranges, KPI sanity checks.
- Visualized trends: close price, rolling Sharpe, year-over-year comparison, and interactive exploration with Plotly.


Next: organize SQL scripts, integrate with dbt, and automate ETL via Airflow.

