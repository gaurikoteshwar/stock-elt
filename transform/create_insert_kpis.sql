/* ============================================================
Incremental KPI Build: analytics.stock_kpis_daily (NVDA)
- Recomputes rolling context (30d buffer)
- Inserts ONLY new dates
============================================================ */

WITH last_kpi_date AS (
    SELECT
        COALESCE(MAX(date), DATE '1900-01-01') AS max_date
    FROM analytics.stock_kpis_daily
    WHERE symbol = 'NVDA'
),

base AS (
    SELECT
        s.symbol,
        s.date,
        s.close_price,
        s.volume,
        LAG(s.close_price) OVER (
            PARTITION BY s.symbol
            ORDER BY s.date
        ) AS prev_close
    FROM staging.stg_stock_prices s
    JOIN last_kpi_date l
      ON s.date >= l.max_date - INTERVAL '30 days'
    WHERE s.symbol = 'NVDA'
),

returns AS (
    SELECT
        *,
        (close_price - prev_close) / NULLIF(prev_close, 0) AS daily_return
    FROM base
),

rolling AS (
    SELECT
        *,
        STDDEV(daily_return) OVER (
            PARTITION BY symbol
            ORDER BY date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS rolling_vol_20d,
        AVG(daily_return) OVER (
            PARTITION BY symbol
            ORDER BY date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS avg_return_20d
    FROM returns
),

obv_calc AS (
    SELECT
        *,
        SUM(
            CASE
                WHEN close_price > prev_close THEN volume
                WHEN close_price < prev_close THEN -volume
                ELSE 0
            END
        ) OVER (
            PARTITION BY symbol
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS obv
    FROM rolling
),

drawdown_calc AS (
    SELECT
        *,
        MAX(close_price) OVER (
            PARTITION BY symbol
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS running_peak
    FROM obv_calc
)

INSERT INTO analytics.stock_kpis_daily (
    symbol,
    date,
    close_price,
    daily_return,
    rolling_vol_20d,
    rolling_sharpe_20d,
    max_drawdown,
    obv
)
SELECT
    d.symbol,
    d.date,
    d.close_price,
    d.daily_return,
    d.rolling_vol_20d,
    CASE
        WHEN d.rolling_vol_20d > 0
        THEN d.avg_return_20d / d.rolling_vol_20d
    END AS rolling_sharpe_20d,
    MIN(
        (d.close_price - d.running_peak) / NULLIF(d.running_peak, 0)
    ) OVER (
        PARTITION BY d.symbol
        ORDER BY d.date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS max_drawdown,
    d.obv
FROM drawdown_calc d
JOIN last_kpi_date l
  ON d.date > l.max_date
WHERE d.rolling_vol_20d IS NOT NULL;