{{ config(
    materialized='table',
    database='cheapshark_iceberg',
    schema='silver',
    format='PARQUET'
) }}

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
FROM {{ ref('bronze_games') }}