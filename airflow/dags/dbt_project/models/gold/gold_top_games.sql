{{ config(
    materialized='table',
    database='cheapshark_iceberg',
    schema='gold',
    format='PARQUET'
) }}

WITH ranked_deals AS (
    SELECT
        d.gameID,
        d.title,
        CASE 
            WHEN d.metacriticScore = 'null' THEN NULL
            ELSE CAST(d.metacriticScore AS INTEGER)
        END as metacriticScore,
        CAST(d.steamRatingPercent AS DOUBLE) as steamRatingPercent,
        d.salePrice,
        d.normalPrice,
        d.savings,
        d.storeID,
        s.storeName,
        ROW_NUMBER() OVER (PARTITION BY d.gameID ORDER BY CAST(d.salePrice AS DOUBLE)) as price_rank
    FROM {{ ref('silver_deals') }} d
    JOIN {{ ref('silver_stores') }} s ON d.storeID = s.storeID
    WHERE d.metacriticScore IS NOT NULL
      AND d.metacriticScore != 'null'
      AND d.steamRatingPercent IS NOT NULL
)

SELECT
    gameID,
    title,
    metacriticScore,
    steamRatingPercent,
    salePrice as best_deal_price,
    normalPrice as original_price,
    storeName as best_deal_store,
    ROUND(CAST(savings AS DOUBLE) * 100, 2) as savings,
    CURRENT_DATE as partition_date,
    CAST(CURRENT_TIMESTAMP AS varchar) as processed_at
FROM ranked_deals
WHERE price_rank = 1
  AND metacriticScore > 0
ORDER BY metacriticScore DESC, steamRatingPercent DESC
LIMIT 100 