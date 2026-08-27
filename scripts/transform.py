import pandas as pd


def transform_energy_data(file_path: str) -> str:
    """
    Pure function — transforms raw hourly energy data into daily regional summaries.
    Input:  raw CSV with ~1947 hourly rows (period, respondent, type, value)
    Output: clean CSV with ~75 daily rows (one per region)
    """
    df = pd.read_csv(file_path)
    df["value"] = df["value"].astype(float)

    # Step 1 — Extract the date from the period column
    df["date"] = pd.to_datetime(df["period"]).dt.date

    # Step 2 — Separate demand and generation
    demand_df = df[df["type"] == "D"].groupby(
        ["date", "respondent"], as_index=False
    )["value"].sum().rename(columns={"value": "total_demand"})

    generation_df = df[df["type"] == "NG"].groupby(
        ["date", "respondent"], as_index=False
    )["value"].sum().rename(columns={"value": "total_generation"})

    # Step 3 — Join demand and generation
    summary_df = pd.merge(demand_df, generation_df, on=["date", "respondent"], how="inner")

    # Step 4 — Calculate net surplus
    summary_df["net_surplus"] = summary_df["total_generation"] - summary_df["total_demand"]

    # Step 5 — Round values to 2 decimal places
    summary_df[["total_demand", "total_generation", "net_surplus"]] = \
        summary_df[["total_demand", "total_generation", "net_surplus"]].round(2)

    # Step 6 — Sort by net_surplus ascending
    summary_df = summary_df.sort_values("net_surplus", ascending=True).reset_index(drop=True)

    # Step 7 — Save to clean CSV
    output_path = file_path.replace("raw", "clean").replace("energy_", "energy_clean_")
    summary_df.to_csv(output_path, index=False)

    print(f"Transformed {len(df)} raw rows → {len(summary_df)} daily summaries")
    print(f"Saved to {output_path}")
    print(f"\nTop 3 deficit regions:\n{summary_df[['respondent','net_surplus']].head(3)}")
    print(f"\nTop 3 surplus regions:\n{summary_df[['respondent','net_surplus']].tail(3)}")

    return output_path


def transform_energy_data_task(file_path: str) -> str:
    return transform_energy_data(file_path)