{{ config(
    materialized='table',
    database='cheapshark_iceberg',
    schema='gold',
    format='PARQUET'
) }}

WITH deals_with_store AS (
    SELECT
        d.dealID,
        d.storeID,
        s.storeName,
        d.salePrice,
        d.normalPrice,
        d.savings,
        d.isOnSale
    FROM {{ ref('silver_deals') }} d
    JOIN {{ ref('silver_stores') }} s ON d.storeID = s.storeID
    WHERE d.dealID IS NOT NULL
)

SELECT
    storeID,
    storeName,
    COUNT(*) as total_deals,
    ROUND(AVG(CAST(savings AS DOUBLE)) * 100, 2) as avg_discount,
    ROUND(AVG(CAST(salePrice AS DOUBLE)), 2) as avg_price,
    MIN(CAST(salePrice AS DOUBLE)) as min_price,
    MAX(CAST(salePrice AS DOUBLE)) as max_price,
    CURRENT_DATE as partition_date,
    CAST(CURRENT_TIMESTAMP AS varchar) as processed_at
FROM deals_with_store
GROUP BY storeID, storeName
ORDER BY total_deals DESC 