1. docker compose up --build
2. docker compose run airflow-init
3. wait 1 min and then access http://localhost:8080
4. Trigger the dag
5. check the logs and the output folder under the spark_app, the logs will contain info about the spark job while the output will contain the aggrgated final txt file
