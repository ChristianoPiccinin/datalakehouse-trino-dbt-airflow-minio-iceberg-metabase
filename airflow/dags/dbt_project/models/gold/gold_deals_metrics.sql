{{ config(
    materialized='table',
    database='cheapshark_iceberg',
    schema='gold',
    format='PARQUET'
) }}

WITH deals_metrics AS (
    SELECT
        CURRENT_DATE as date,
        COUNT(*) as total_active_deals,
        ROUND(AVG(CAST(savings AS DOUBLE)) * 100, 2) as avg_discount_percentage,
        COUNT(DISTINCT storeID) as total_stores_with_deals
    FROM {{ ref('silver_deals') }}
    WHERE dealID IS NOT NULL
),

-- Categorização dos jogos baseada em metacriticScore
game_categories AS (
    SELECT
        CASE
            WHEN TRY_CAST(metacriticScore AS INTEGER) >= 90 THEN 'AAA'
            WHEN TRY_CAST(metacriticScore AS INTEGER) >= 75 THEN 'High Quality'
            WHEN TRY_CAST(metacriticScore AS INTEGER) >= 60 THEN 'Good'
            ELSE 'Average/Below'
        END as category,
        AVG(CAST(savings AS DOUBLE)) * 100 as avg_category_discount
    FROM {{ ref('silver_deals') }}
    WHERE metacriticScore IS NOT NULL
      AND metacriticScore != 'null'
    GROUP BY 
        CASE
            WHEN TRY_CAST(metacriticScore AS INTEGER) >= 90 THEN 'AAA'
            WHEN TRY_CAST(metacriticScore AS INTEGER) >= 75 THEN 'High Quality'
            WHEN TRY_CAST(metacriticScore AS INTEGER) >= 60 THEN 'Good'
            ELSE 'Average/Below'
        END
),

-- Identificando a categoria com maior desconto médio
top_category AS (
    SELECT
        category as most_discounted_category
    FROM game_categories
    ORDER BY avg_category_discount DESC
    LIMIT 1
)

SELECT
    dm.date,
    dm.total_active_deals,
    dm.avg_discount_percentage,
    dm.total_stores_with_deals,
    tc.most_discounted_category,
    CURRENT_DATE as partition_date,
    CAST(CURRENT_TIMESTAMP AS varchar) as processed_at
FROM deals_metrics dm
CROSS JOIN top_category tc 