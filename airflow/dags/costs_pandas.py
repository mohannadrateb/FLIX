from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2025, 7, 10),
    'owner': 'mohannadrateb'
}

with DAG(
    'submit_pandas_job',
    default_args=default_args,
    schedule_interval='55 19 * * *',
    catchup=False,
) as dag:
    
    submit_job = BashOperator(
        task_id='submit_pandas_job',
        bash_command="""
            python3 /app/pandas_app/pandas_transformation.py /app/data/raw_costs.csv /app/data/accounts_info.csv /app/data/exchange_rates.csv 2>&1
        """
    )
