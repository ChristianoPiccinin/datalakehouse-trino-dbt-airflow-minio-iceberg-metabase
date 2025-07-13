from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from cosmos import DbtTaskGroup, ProfileConfig, ProjectConfig
from pathlib import Path

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 7, 13),
    "retries": 1,
}

DBT_PATH = "/opt/airflow/dags/dbt_project/.dbt"
DBT_PATH_PROJECT = "/opt/airflow/dags/dbt_project"
  # Removido trailing slash
DBT_PROFILE = "cheapshark"  # Deve corresponder ao nome no profiles.yml
DBT_TARGET = "dev"       # Corrigido nome da variável

with DAG(
    dag_id="dbt_dag_workflow",
    default_args=default_args,
    description="A DAG to run dbt commands",
    schedule=None,
    tags=["dbt"],
) as dag:

    profile_config = ProfileConfig(
        profile_name=DBT_PROFILE,
        target_name=DBT_TARGET,
        profiles_yml_filepath=Path(f"{DBT_PATH}/profiles.yml"),
    )

    project_config = ProjectConfig(
        dbt_project_path=Path(DBT_PATH_PROJECT),
        models_relative_path="models"
    )

    start_dag = EmptyOperator(task_id="start_dag")

    dbt_running_models = DbtTaskGroup(
        group_id="dbt_running_models",
        project_config=project_config,
        profile_config=profile_config,
        default_args={"retries": 2},
    )

    end_dag = EmptyOperator(task_id="end_dag")

    start_dag >> dbt_running_models >> end_dag