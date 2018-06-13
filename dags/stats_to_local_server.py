import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from druid_operator import DruidStatsOperator
from converters import visualize_winrates

default_args = {
    'owner': 'user',
    'depends_on_past': False,
    'email': ['email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

PROJECT_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)))
WWW_DATA_DIR = os.path.join(PROJECT_DIR, 'www')
INDEX_FILE = os.path.join(PROJECT_DIR, 'www', 'index.html')

with DAG(
        dag_id='winrate-from-druid',
        schedule_interval='@daily',
        start_date=datetime(2018, 6, 1),
        default_args=default_args,
        max_active_runs=1,
        catchup=True,
) as dag:
    ensure_data_dir_exists_cmd = """
    mkdir -p {WWW_DATA_DIR}/{{ ds }}
    """
    ensure_data_dir_exists = BashOperator(
        task_id='ensure_stats_dir_exists',
        bash_command=ensure_data_dir_exists_cmd,
        dag=dag
    )

    get_winrates_from_druid = DruidStatsOperator(
        task_id='download_winrates_from_druid',
        intervals="{{ ds }}/{{ tomorrow_ds }}",
        druid_broker_conn_id='druid_broker_default',
        dag=dag
    )

    convert_stats_to_viz = PythonOperator(
        task_id='convert_csv_to_visualisation',
        provide_context=True,
        python_callable=visualize_winrates,
        dag=dag
    )

    update_index_page = BashOperator(
        task_id='update_index_page',
        bash_command='ls {WWW_DATA_DIR} | {PROJECT_DIR}/update_server {INDEX_FILE}'.format(
            WWW_DATA_DIR,
            PROJECT_DIR,
            INDEX_FILE
        ),
        dag=dag
    )

    ensure_data_dir_exists >> get_winrates_from_druid >> convert_stats_to_viz >> update_index_page
