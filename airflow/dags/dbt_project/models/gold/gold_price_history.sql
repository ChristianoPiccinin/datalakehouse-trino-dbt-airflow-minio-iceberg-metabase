{{ config(
    materialized='incremental',
    database='cheapshark_iceberg',
    schema='gold',
    format='PARQUET',
    unique_key='id'
) }}

WITH current_prices AS (
    SELECT
        CONCAT(gameID, '-', storeID, '-', CAST(CURRENT_DATE AS VARCHAR)) as id,
        gameID,
        title,
        storeID,
        CAST(salePrice AS DOUBLE) as price,
        CAST(normalPrice AS DOUBLE) as original_price,
        CAST(savings AS DOUBLE) * 100 as discount_percentage,
        CURRENT_DATE as price_date,
        CAST(CURRENT_TIMESTAMP AS varchar) as processed_at
    FROM {{ ref('silver_deals') }}
    WHERE dealID IS NOT NULL
)

SELECT * FROM current_prices

{% if is_incremental() %}
    WHERE price_date > (SELECT MAX(price_date) FROM {{ this }})
{% endif %} 