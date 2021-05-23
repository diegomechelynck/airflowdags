from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator, DatabricksRunNowOperator
from datetime import datetime, timedelta 


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

with DAG('databricks_dieg_dag',
    start_date=datetime(2021, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args
    ) as dag:
    helloworld1 = DatabricksRunNowOperator(
        task_id='run_now',
        databricks_conn_id='Databricksdieg',
        job_id=5
    )

    helloworld2 = BashOperator(
            task_id='running_bash_bitch',
            bash_command='echo donneeeeeeeeeeeeeeeee',
        )

    helloworld1 >> helloworld2
