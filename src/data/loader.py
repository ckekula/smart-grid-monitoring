from pathlib import Path

import pandas as pd

DATA_DIR = Path(__file__).resolve().parent.parent
DATASET = DATA_DIR / "smart_grid_dataset_new.csv"

def load_dataset(file_path: Path) -> pd.DataFrame:
    if not file_path.exists():
        raise FileNotFoundError(f"Dataset not found: {file_path}")

    df = pd.read_csv(file_path)

    df["Timestamp"] = pd.to_datetime(df["Timestamp"], errors="raise")
    df["Overload Condition"] = df["Overload Condition"].astype(bool)
    df["Transformer Fault"] = df["Transformer Fault"].astype(bool)

    return df


if __name__ == "__main__":
    dataset = load_dataset()

    print(f"Records: {len(dataset):,}")
    print(f"Meters: {dataset['Meter ID'].nunique()}")
    print(f"Households: {dataset['Household ID'].nunique()}")
    print(f"Zones: {dataset['Grid Zone'].unique().tolist()}")
    print(f"Start: {dataset['Timestamp'].min()}")
    print(f"End: {dataset['Timestamp'].max()}")
