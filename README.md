### 1. Build the Docker containers
    docker compose build 
(This step may take 15 to 20 minutes)    
### 2. Initlialize Airflow
    docker compose run airflow-init
### 3. Start the container
    docker compose up
### 4. Access the Airflow UI
After the airflow_webserver is up, check: \
http://localhost:8080   username and password: admin

### 5. Tigger the Dag 
In the Airflow UI, you'll find 2 DAGs:

- submit_pandas_job

- submit_spark_job

### 6. Check the output
- Pandas DAG: submit_pandas_job
    - Input: Files from the data/ folder

    - Logs: Found in pandas_app/logs/

    - Output: Result saved to pandas_app/output/daily_spend.txt

- PySpark DAG: submit_spark_job
    - Input: Files from the data/ folder

    - Logs: Found in spark_app/logs/

    - Output: Result saved to spark_app/output/daily_spend_spark.txt


### 7. checking the code
- Pandas Pipeline Code:
    - pandas_app/pandas_transformation.py

- PySpark Pipeline Code:
    - spark_app/pyspark_transformation.py
Both scripts are documented and follow modular structure.
### 8. check Test
Test scripts are included to verify both input and output data quality.
- Pandas Tests: pandas_app/tests.py
- PySpark Tests: spark_app/tests_pyspark.py






