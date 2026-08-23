from airflow.sdk import dag, task
from pendulum import datetime
from scripts.extract import extract_energy_data


@dag(
    dag_id="energy_pipeline",
    start_date=datetime(year=2026, month=8, day=22, tz="UTC"),
    schedule="@daily",
    catchup=False,
    is_paused_upon_creation=False,
    tags=["energy", "eia", "portfolio"],
)
def energy_pipeline():

    @task.python
    def extract():
        return extract_energy_data()

    extract()


energy_pipeline()