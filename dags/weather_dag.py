from airflow import DAG
# from airflow.operators.bash_operator import BashOperator
from airflow.providers.standard.operators.bash import BashOperator 
# from airflow.operators.empty import EmptyOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime

# for dbt
DBT_PROJECT_DIR = "/opt/airflow/weather_data_models"
DBT_PROFILES_DIR = "/opt/airflow/"


with DAG(
    dag_id='weather_dag',
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False
):
    
    start = EmptyOperator(
        task_id = 'start'
    )

    extract_and_load_to_duckdb = BashOperator(
        task_id = "extract_and_load_to_duckdb",

        # since dags folder is already mounted as voulme in yml, any script we place in dags folder will be available in below path
        bash_command = "python /opt/airflow/dags/data_ingestion.py"
    )

    dbt_deps = BashOperator(
        task_id = "dbt_deps_installation",
        # bash_command = f"cd {DBT_PROJECT_DIR} && dbt deps --profiles-dir {DBT_PROFILES_DIR}"
        # to only run dbt deps on the first run or any new poackages is installed in packages.yml
        bash_command=f"cd {DBT_PROJECT_DIR} && if [ ! -d 'dbt_packages' ] || [ packages.yml -nt dbt_packages ]; then dbt deps --profiles-dir {DBT_PROFILES_DIR}; else echo 'Packages up to date. Skipping download.'; fi"
    )

    dbt_pre_stage_test = BashOperator(
        task_id = "dbt_pre_stage_test",
        bash_command = f"cd {DBT_PROJECT_DIR} && dbt test --select source:* --profiles-dir {DBT_PROFILES_DIR}"
    )

    dbt_transform = BashOperator(
        task_id="dbt_transform",
        bash_command = f"cd {DBT_PROJECT_DIR} && dbt run --profiles-dir {DBT_PROFILES_DIR}"
    )


    dbt_mart_layer_test = BashOperator(
        task_id = "dbt_mart_layer_test",
        bash_command = f"cd {DBT_PROJECT_DIR} && dbt test --select marts --profiles-dir {DBT_PROFILES_DIR}"
    )

    end = EmptyOperator(
        task_id = 'end'
    )


    start >> extract_and_load_to_duckdb >> dbt_deps >> dbt_pre_stage_test >> dbt_transform >> dbt_mart_layer_test >> end
