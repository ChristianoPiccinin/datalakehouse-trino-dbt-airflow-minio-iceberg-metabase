{{ config(
    materialized='table',
    database='cheapshark_iceberg',
    schema='bronze',
    format='PARQUET'
) }}

WITH source_data AS (
    SELECT 
        CAST(storeID AS VARCHAR) as storeID,
        storeName,
        isActive,
        images_banner,
        images_logo,
        images_icon,
        ingested_at
    FROM {{ source('staging', 'stores') }}
    WHERE ingested_at IS NOT NULL
)

SELECT 
    CAST(storeID AS VARCHAR) as storeID,
    storeName,
    isActive,
    images_banner,
    images_logo,
    images_icon,
    CAST(ingested_at AS varchar) as ingested_at,
    CURRENT_DATE as partition_date,
    CAST(CURRENT_TIMESTAMP AS varchar) as processed_at
FROM source_data