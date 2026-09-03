import pandas as pd
from sqlalchemy import create_engine, text


def get_engine():
    return create_engine(
        "postgresql+psycopg2://airflow:airflow@postgres/airflow"
    )


def quality_check_energy_data(file_path: str) -> str:
    """
    Queries PostgreSQL directly to verify the load actually worked correctly.
    Raises ValueError if any check fails.
    """
    df = pd.read_csv(file_path)
    date = str(df["date"].iloc[0])
    expected_rows = len(df)

    engine = get_engine()

    with engine.connect() as conn:

        # Check 1 — Row count matches what was loaded
        result = conn.execute(
            text("SELECT COUNT(*) FROM daily_energy_summary WHERE date = :date"),
            {"date": date}
        )
        actual_rows = result.scalar()
        if actual_rows != expected_rows:
            raise ValueError(
                f"Row count mismatch for {date}: "
                f"expected {expected_rows}, found {actual_rows} in database"
            )

        # Check 2 — No nulls in key columns
        result = conn.execute(
            text("""
                SELECT COUNT(*) FROM daily_energy_summary
                WHERE date = :date
                AND (
                    total_demand IS NULL OR
                    total_generation IS NULL OR
                    net_surplus IS NULL
                )
            """),
            {"date": date}
        )
        null_count = result.scalar()
        if null_count > 0:
            raise ValueError(f"Found {null_count} rows with null values for {date}")

        # Check 3 — Net surplus math is correct
        result = conn.execute(
            text("""
                SELECT COUNT(*) FROM daily_energy_summary
                WHERE date = :date
                AND ABS((total_generation - total_demand) - net_surplus) > 1
            """),
            {"date": date}
        )
        math_errors = result.scalar()
        if math_errors > 0:
            raise ValueError(
                f"Net surplus calculation error in {math_errors} rows for {date}"
            )

        # Check 4 — Today's date exists in the table
        result = conn.execute(
            text("SELECT COUNT(*) FROM daily_energy_summary WHERE date = :date"),
            {"date": date}
        )
        date_count = result.scalar()
        if date_count == 0:
            raise ValueError(f"No rows found for date {date} — load may have silently failed")

    print(f"Quality check passed for {date} — {actual_rows} rows verified in PostgreSQL")
    return file_path


def quality_check_energy_data_task(file_path: str) -> str:
    return quality_check_energy_data(file_path)