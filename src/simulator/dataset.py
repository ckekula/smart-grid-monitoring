from pathlib import Path

import pandas as pd


def load_dataset(path: str) -> pd.DataFrame:
    dataset_path = Path(path)

    if not dataset_path.exists():
        raise FileNotFoundError(f"Dataset not found: {dataset_path}")

    df = pd.read_csv(dataset_path)

    df["Timestamp"] = pd.to_datetime(df["Timestamp"], errors="raise")
    df = df.sort_values("Timestamp").reset_index(drop=True)

    return df


def clean_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    numeric_columns = [
        "Voltage (V)",
        "Current (A)",
        "Power Consumption (kW)",
        "Reactive Power (kVAR)",
        "Power Factor",
        "Solar Power (kW)",
        "Wind Power (kW)",
        "Grid Supply (kW)",
        "Voltage Fluctuation (%)",
        "Temperature (°C)",
        "Humidity (%)",
        "Electricity Price (USD/kWh)",
        "Predicted Load (kW)",
    ]

    for column in numeric_columns:
        df[column] = pd.to_numeric(df[column], errors="raise")

    return df


def prepare_dataset(path: str) -> pd.DataFrame:
    df = load_dataset(path)
    df = clean_numeric_columns(df)

    # Remove rows that don't have the required core measurements
    df = df.dropna(subset=[
            "Power Consumption (kW)",
            "Solar Power (kW)",
            "Grid Supply (kW)",
        ])

    return df