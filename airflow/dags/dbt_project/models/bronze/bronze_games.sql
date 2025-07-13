{{ config(
    materialized='table',
    database='cheapshark_iceberg',
    schema='bronze',
    format='PARQUET'
) }}

WITH source_data AS (
    SELECT 
        title,
        CAST(steamAppID AS VARCHAR) as steamAppID,
        thumb,
        price,
        deals,
        CAST(gameID AS VARCHAR) as gameID,
        ingested_at
    FROM {{ source('staging', 'games') }}
    WHERE ingested_at IS NOT NULL
)

SELECT 
    title,
    steamAppID,
    thumb,
    price,
    deals,
    gameID,
    CAST(ingested_at AS varchar) as ingested_at,
    CURRENT_DATE as partition_date,
    CAST(CURRENT_TIMESTAMP AS varchar) as processed_at
FROM source_data