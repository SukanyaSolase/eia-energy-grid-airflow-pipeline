import pandas as pd
from sqlalchemy import create_engine, text


def get_engine():

    return create_engine(
        "postgresql+psycopg2://airflow:airflow@postgres/airflow"
    )


def create_table_if_not_exists(engine):
    
    create_table_sql = """
        CREATE TABLE IF NOT EXISTS daily_energy_summary (
            date               DATE         NOT NULL,
            respondent         VARCHAR(20)  NOT NULL,
            total_demand       NUMERIC(15, 2),
            total_generation   NUMERIC(15, 2),
            net_surplus        NUMERIC(15, 2),
            PRIMARY KEY (date, respondent)
        );
    """
    with engine.connect() as conn:
        conn.execute(text(create_table_sql))
        conn.commit()
    print("Table daily_energy_summary ready")


def upsert_data(df: pd.DataFrame, engine):
    """
    Upserts daily summaries into PostgreSQL.
    """
    upsert_sql = """
        INSERT INTO daily_energy_summary
            (date, respondent, total_demand, total_generation, net_surplus)
        VALUES
            (:date, :respondent, :total_demand, :total_generation, :net_surplus)
        ON CONFLICT (date, respondent)
        DO UPDATE SET
            total_demand     = EXCLUDED.total_demand,
            total_generation = EXCLUDED.total_generation,
            net_surplus      = EXCLUDED.net_surplus;
    """
    with engine.connect() as conn:
        for _, row in df.iterrows():
            conn.execute(text(upsert_sql), {
                "date":             str(row["date"]),
                "respondent":       str(row["respondent"]),
                "total_demand":     float(row["total_demand"]),
                "total_generation": float(row["total_generation"]),
                "net_surplus":      float(row["net_surplus"]),
            })
        conn.commit()
    print(f"Upserted {len(df)} rows into daily_energy_summary")


def load_energy_data(file_path: str) -> str:

    df = pd.read_csv(file_path)

    engine = get_engine()
    create_table_if_not_exists(engine)
    upsert_data(df, engine)

    print(f"Load complete — {len(df)} rows for {df['date'].iloc[0]}")
    return file_path


def load_energy_data_task(file_path: str) -> str:
    
    return load_energy_data(file_path)