from datetime import datetime

from airflow.sdk import dag, task


@dag(
    dag_id="hello_pipeline",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["learning"],
)
def hello_pipeline():
    @task
    def say_hello():
        print("Hello from the market data pipeline.")
        return "airflow-is-working"

    @task
    def confirm_result(message: str):
        print(f"Received from upstream task: {message}")

    result = say_hello()
    confirm_result(result)


hello_pipeline()