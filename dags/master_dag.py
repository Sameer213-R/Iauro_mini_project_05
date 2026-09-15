from airflow.sdk import dag
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
import pendulum


@dag(
    dag_id="master_dag",
    start_date=pendulum.datetime(2026, 9, 15, tz="Asia/Kolkata"),
    schedule=None,
    catchup=False
)
def master_dag():

    # Trigger the API fetching DAG
    fetch_api = TriggerDagRunOperator(
        task_id="trigger_fetching_api",
        trigger_dag_id="fetching_gitAPI_data",
        wait_for_completion=True
    )

    # Trigger the transformation DAG
    transformation = TriggerDagRunOperator(
        task_id="trigger_transformation",
        trigger_dag_id="processing_data_with_pandas",
        wait_for_completion=True
    )

    # Dependency
    fetch_api >> transformation


master_dag()