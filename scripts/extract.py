import requests
import pandas as pd

def fetch_energy_data(start_date: str, end_date: str, api_key: str) -> pd.DataFrame:
    """
    Pure function — no Airflow dependency.
    Fetches hourly demand and net generation data from EIA API.
    Testable standalone without Airflow running.
    """
    url = "https://api.eia.gov/v2/electricity/rto/region-data/data/"
    all_records = []

    for data_type in ["D", "NG"]:  # D = Demand, NG = Net Generation
        params = {
            "api_key": api_key,
            "frequency": "hourly",
            "data[]": "value",
            "facets[type][]": data_type,
            "start": start_date,
            "end": end_date,
            "sort[0][column]": "period",
            "sort[0][direction]": "asc",
            "length": 5000,
        }
        response = requests.get(url, params=params)
        response.raise_for_status()
        all_records.extend(response.json()["response"]["data"])

    df = pd.DataFrame(all_records)

    # Keep only fact columns — drop dimension data (respondent-name, type-name, value-units)
    df = df[["period", "respondent", "type", "value"]]

    return df


def extract_energy_data():
    """
    Airflow wrapper — fetches context + secret, calls pure function, saves CSV.
    This is what the DAG task actually calls.
    """
    from airflow.sdk import get_current_context
    from airflow.sdk import Variable

    context = get_current_context()
    data_interval_start = context["data_interval_start"]
    data_interval_end = context["data_interval_end"]

    start_str = data_interval_start.strftime("%Y-%m-%d")
    end_str = data_interval_end.strftime("%Y-%m-%d")

    api_key = Variable.get("eia_api_key")

    df = fetch_energy_data(start_str, end_str, api_key)

    output_path = f"/opt/airflow/data/raw/energy_{start_str}.csv"
    df.to_csv(output_path, index=False)

    print(f"Extracted {len(df)} rows for {start_str}")
    print(f"Saved to {output_path}")

    return output_path