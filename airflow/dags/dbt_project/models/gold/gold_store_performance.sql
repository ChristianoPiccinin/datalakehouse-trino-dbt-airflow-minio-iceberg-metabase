{{ config(
    materialized='table',
    database='cheapshark_iceberg',
    schema='gold',
    format='PARQUET'
) }}

WITH store_deals AS (
    SELECT
        s.storeID,
        s.storeName,
        COUNT(d.dealID) as total_deals,
        ROUND(AVG(CAST(d.savings AS DOUBLE)) * 100, 2) as avg_discount,
        COUNT(CASE WHEN CAST(d.savings AS DOUBLE) > 0.5 THEN 1 END) as high_discount_deals,
        ROUND(AVG(CAST(d.dealRating AS DOUBLE)), 2) as avg_deal_rating
    FROM {{ ref('silver_stores') }} s
    LEFT JOIN {{ ref('silver_deals') }} d ON s.storeID = d.storeID
    WHERE d.dealID IS NOT NULL
    GROUP BY s.storeID, s.storeName
),

store_metrics AS (
    SELECT
        storeID,
        storeName,
        total_deals,
        avg_discount,
        high_discount_deals,
        avg_deal_rating,
        ROUND(CAST(high_discount_deals AS DOUBLE) / NULLIF(total_deals, 0) * 100, 2) as pct_high_discount,
        CASE
            WHEN avg_deal_rating >= 8 THEN 'Excellent'
            WHEN avg_deal_rating >= 6 THEN 'Good'
            WHEN avg_deal_rating >= 4 THEN 'Average'
            ELSE 'Below Average'
        END as performance_category
    FROM store_deals
)

SELECT
    storeID,
    storeName,
    total_deals,
    avg_discount,
    high_discount_deals,
    pct_high_discount,
    avg_deal_rating,
    performance_category,
    CURRENT_DATE as partition_date,
    CAST(CURRENT_TIMESTAMP AS varchar) as processed_at
FROM store_metrics
ORDER BY avg_deal_rating DESC 