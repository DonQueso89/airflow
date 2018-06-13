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

with DAG(
        dag_id='winrate-from-druid',
        schedule_interval='@daily',
        start_date=datetime(2018, 6, 1),
        default_args=default_args,
        max_active_runs=1,
        catchup=True,
) as dag:
    ensure_data_dir_exists_cmd = """
    mkdir -p /tmp/winrate_stats/{{ ds }}
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

    ensure_data_dir_exists >> get_winrates_from_druid >> convert_stats_to_viz
