{{ config(
    materialized='table',
    database='cheapshark_iceberg',
    schema='bronze',
    format='PARQUET'
) }}

WITH source_data AS (
    SELECT 
        CAST(internalName AS VARCHAR) as internalName,
        title,
        metacriticLink,
        dealID,
        CAST(storeID AS VARCHAR) as storeID,
        CAST(gameID AS VARCHAR) as gameID,
        CAST(salePrice AS DECIMAL(10, 2)) as salePrice,
        CAST(normalPrice AS DECIMAL(10, 2)) as normalPrice,
        isOnSale,
        savings,
        metacriticScore,
        steamRatingText,
        steamRatingPercent,
        steamRatingCount,
        steamAppID,
        releaseDate,
        lastChange,
        dealRating,
        thumb,
        ingested_at
    FROM {{ source('staging', 'deals') }}
    WHERE ingested_at IS NOT NULL
)

SELECT 
    CAST(internalName AS VARCHAR) as internalName,
    title,
    metacriticLink,
    dealID,
    CAST(storeID AS VARCHAR) as storeID,
    CAST(gameID AS VARCHAR) as gameID,
    CAST(salePrice AS DECIMAL(10, 2)) as salePrice,
    CAST(normalPrice AS DECIMAL(10, 2)) as normalPrice,
    isOnSale,
    CAST(savings AS DECIMAL(10, 2)) as savings,
    metacriticScore,
    steamRatingText,
    CAST(steamRatingPercent AS DECIMAL(10, 2)) as steamRatingPercent,
    CAST(steamRatingCount AS INTEGER) as steamRatingCount,
    CAST(steamAppID AS VARCHAR) as steamAppID,
    CAST(releaseDate AS varchar) as releaseDate,
    CAST(lastChange AS varchar) as lastChange,
    dealRating,
    thumb,
    CAST(ingested_at AS varchar) as ingested_at,
    CURRENT_DATE as partition_date,
    CAST(CURRENT_TIMESTAMP AS varchar) as processed_at
FROM source_data