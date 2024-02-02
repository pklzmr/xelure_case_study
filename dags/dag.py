from datetime import datetime, timedelta
from airflow import DAG
import os
from airflow.operators.python import PythonOperator

default_args = {
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'xelure_data_validation',
    default_args=default_args,
    schedule_interval=timedelta(days=1),  # Run the DAG daily
)

data_quality_check_task = PythonOperator(
    task_id='data_quality_check_task',
    python_callable= lambda: os.system("python3 /Users/pklzmr/Desktop/Xelur Case Study/xelure_case_study/dq_check.py"),
    dag=dag,
)

validate_all_dates_task = PythonOperator(
    task_id='validate_all_dates_task',
    python_callable=lambda: os.system("python3 /Users/pklzmr/Desktop/Xelur Case Study/xelure_case_study/main.py --validate_all_dates"),
    dag=dag,
)

data_quality_check_task >> validate_all_dates_task

if __name__ == "__main__":
    dag.cli()
