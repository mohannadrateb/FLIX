from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2025, 7, 10),
    'owner': 'mohannadrateb'
}

with DAG(
    'submit_spark_job',
    default_args=default_args,
    schedule_interval='55 19 * * *',
    catchup=False,
) as dag:
    
    submit_job = BashOperator(
        
        task_id='submit_pyspark_job',
        
        bash_command="""
        # Set Java environment variables
        export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))
        export PATH=$JAVA_HOME/bin:$PATH
        
        /opt/spark/bin/spark-submit /app/spark_app/pyspark_transformation.py /app/data/raw_costs.csv /app/data/accounts_info.csv /app/data/exchange_rates.csv 2>&1 | grep -vE 'WARN|DEBUG'
        """
    )
