from airflow.decorators import dag, task
from airflow.providers.trino.hooks.trino import TrinoHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="create_schema_staging_tables",
    start_date=datetime(2025, 7, 13),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["trino", "schema", "staging"]
)
def create_data_warehouse():
    
    create_schema_staging = SQLExecuteQueryOperator(
        task_id="create_schema_staging",
        conn_id="trino_default",
        sql="""
            CREATE SCHEMA IF NOT EXISTS cheapshark_staging.staging
            WITH (location = 's3a://cheapshark-warehouse/staging')
        """,
        handler=list,
    )


    create_table_staging_deals = SQLExecuteQueryOperator(
        task_id="create_table_staging_deals",
        conn_id="trino_default",
        sql="""
            CREATE TABLE IF NOT EXISTS cheapshark_staging.staging.deals
            (
             internalName VARCHAR,
             title VARCHAR,
             metacriticLink VARCHAR,
             dealID VARCHAR,
             storeID VARCHAR,
             gameID VARCHAR,
             salePrice VARCHAR,
             normalPrice VARCHAR,
             isOnSale VARCHAR,
             savings VARCHAR,
             metacriticScore VARCHAR,
             steamRatingText VARCHAR,
             steamRatingPercent VARCHAR,
             steamRatingCount VARCHAR,
             steamAppID VARCHAR,
             releaseDate VARCHAR,
             lastChange VARCHAR,
             dealRating VARCHAR,
             thumb VARCHAR,
             ingested_at VARCHAR
            )
            WITH (
                external_location = 's3a://cheapshark-warehouse/raw/cheapshark/deals',
                format = 'PARQUET'
            )
        """,
        handler=list,
    )

    create_table_staging_games = SQLExecuteQueryOperator(
        task_id="create_table_staging_games",
        conn_id="trino_default",
        sql="""
            CREATE TABLE IF NOT EXISTS cheapshark_staging.staging.games
            (
                title VARCHAR,
                steamAppID VARCHAR,
                thumb VARCHAR,
                price VARCHAR,
                deals VARCHAR,
                gameID VARCHAR,
                ingested_at VARCHAR
            )
            WITH (
                external_location = 's3a://cheapshark-warehouse/raw/cheapshark/games',
                format = 'PARQUET'
            )
        """,
        handler=list,
    )
  
  

    create_table_staging_stores = SQLExecuteQueryOperator(
        task_id="create_table_staging_stores",
        conn_id="trino_default",
        sql="""
            CREATE TABLE IF NOT EXISTS cheapshark_staging.staging.stores
            (
                storeID VARCHAR,
                storeName VARCHAR,
                isActive VARCHAR,
                images_banner VARCHAR,
                images_logo VARCHAR,
                images_icon VARCHAR,
                ingested_at VARCHAR
            )
            WITH (
                external_location = 's3a://cheapshark-warehouse/raw/cheapshark/stores',
                format = 'PARQUET'
            )
        """,
        handler=list,
    )
    
    # Definindo a dependência entre as tasks
    create_schema_staging >> create_table_staging_deals >> create_table_staging_games >> create_table_staging_stores

# Instanciando o DAG
dag = create_data_warehouse()