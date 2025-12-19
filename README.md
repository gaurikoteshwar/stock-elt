# stock-elt
Build a reliable ELT pipeline for historical and daily stock price data, enabling analytics-ready KPIs and scenario analysis.

This project builds a reliable ELT pipeline to ingest historical and daily stock price data, store it in PostgreSQL as a raw source of truth, and establish a strong foundation for analytics-ready KPI tables and scenario-based analysis.

The primary focus is on correctness, idempotency, and maintainable data modeling rather than downstream analytics or visualisation.

Phase 1 Scope Phase 1 - done
This phase focuses exclusively on data ingestion and raw data reliability.
Included in scope:
- Ingest historical and daily stock price data from the yfinance API
- Load raw, append-only stock price data into PostgreSQL
- Support idempotent daily updates (safe to re-run without duplication)
- Define and enforce a stable raw data contract

Planned Future Phases:
The following are intentionally excluded from Phase 1 and will be added later:
- Business KPI tables
- dbt staging and transformation models
- Monte Carlo simulations and what-if analysis
- Dashboards and visualisations
- Workflow orchestration (e.g. Airflow: requiring Python 3.13 and below, my current version 3.14.2)

High-Level Architecture

yfinance API -> Python (Extract + Load) -> PostgreSQL (raw.stock_prices)


At this stage, no transformations or aggregations are applied to the data.

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
