from airflow.sdk import dag, task
from pendulum import datetime
from scripts.extract import extract_energy_data
from scripts.validate import validate_energy_data_task
from scripts.transform import transform_energy_data_task
from scripts.load import load_energy_data_task
from scripts.quality_check import quality_check_energy_data_task


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

    @task.python
    def validate(file_path: str):
        return validate_energy_data_task(file_path)

    @task.python
    def transform(file_path: str):
        return transform_energy_data_task(file_path)

    @task.python
    def load(file_path: str):
        return load_energy_data_task(file_path)

    @task.python
    def quality_check(file_path: str):
        return quality_check_energy_data_task(file_path)

    raw_path = extract()
    validated_path = validate(raw_path)
    clean_path = transform(validated_path)
    loaded_path = load(clean_path)
    quality_check(loaded_path)


energy_pipeline()