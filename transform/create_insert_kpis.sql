/* Creating KPI definitions and inserting into the table*/
WITH base AS (
    SELECT
        symbol,
        date,
        close_price,
        volume,
        LAG(close_price) OVER (PARTITION BY symbol ORDER BY date) AS prev_close
    FROM staging.stg_stock_prices
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
    symbol,
    date,
    close_price,
    daily_return,
    rolling_vol_20d,
    CASE 
        WHEN rolling_vol_20d > 0 AND rolling_vol_20d IS NOT NULL 
        THEN avg_return_20d / rolling_vol_20d 
        ELSE 0 
    END AS rolling_sharpe_20d,
    MIN( (close_price - running_peak) / NULLIF(running_peak, 0) ) 
        OVER (
            PARTITION BY symbol 
            ORDER BY date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS max_drawdown,
    obv
FROM drawdown_calc
WHERE rolling_vol_20d IS NOT NULL;

-- Ensures full 20-day windows