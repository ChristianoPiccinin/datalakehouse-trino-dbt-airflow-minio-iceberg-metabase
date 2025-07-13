{{ config(
    materialized='table',
    database='cheapshark_iceberg',
    schema='silver',
    format='PARQUET'
) }}

SELECT 
    internalName,
    title,
    metacriticLink,
    dealID,
    storeID,
    gameID,
    salePrice,
    normalPrice,
    isOnSale,
    savings,
    metacriticScore,
    steamRatingText,
    steamRatingPercent,
    steamRatingCount,
    steamAppID,
    CAST(releaseDate AS varchar) as releaseDate,
    CAST(lastChange AS varchar) as lastChange,
    dealRating,
    thumb,
    CAST(ingested_at AS varchar) as ingested_at,
    CURRENT_DATE as partition_date,
    CAST(CURRENT_TIMESTAMP AS varchar) as processed_at
FROM {{ ref('bronze_deals') }}



