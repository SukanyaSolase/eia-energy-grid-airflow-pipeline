import pandas as pd


def validate_energy_data(file_path: str) -> str:

    df = pd.read_csv(file_path)

    # Check 1 — Null values
    null_counts = df.isnull().sum()
    if null_counts.any():
        raise ValueError(f"Null values found:\n{null_counts[null_counts > 0]}")

    # Check 2 — Negative values
    negative_rows = df[df["value"].astype(float) < 0]
    if not negative_rows.empty:
        raise ValueError(f"Negative values found in {len(negative_rows)} rows")

    # Check 3 — Duplicate records
    duplicates = df.duplicated(subset=["period", "respondent", "type"])
    if duplicates.any():
        raise ValueError(f"Duplicate records found: {duplicates.sum()} rows")

    # Check 4 — Both types present (D and NG)
    types_present = df["type"].unique()
    for expected_type in ["D", "NG"]:
        if expected_type not in types_present:
            raise ValueError(f"Missing data type: {expected_type} not found in file")

    # Check 5 — Minimum region count
    region_count = df["respondent"].nunique()
    if region_count < 30:
        raise ValueError(f"Too few regions: expected at least 30, got {region_count}")

    # Check 6 — Value range check
    AGGREGATE_RESPONDENTS = ["US48", "US49", "CAL", "CAR", "CENT", "FLA", "MIDA",
                         "MIDW", "NE", "NW", "NY", "SE", "SW", "TEN", "TEX"]
    
    individual_df = df[~df["respondent"].isin(AGGREGATE_RESPONDENTS)]
    max_value = individual_df["value"].astype(float).max()
    if max_value > 500000:
        raise ValueError(
            f"Unrealistic value in individual region: {max_value} MWh exceeds 500,000 MWh threshold"
        )
    
    print(f"Validation passed — {len(df)} rows, {region_count} regions, types: {list(types_present)}")
    return file_path


def validate_energy_data_task(file_path: str) -> str:
    """
    Airflow wrapper — thin, just calls the pure function.
    """
    return validate_energy_data(file_path)