import csv
import os
import time
from datetime import datetime
from pathlib import Path

SOURCE_FILE = os.getenv(
    "SOURCE_FILE",
    "src/smart_grid_dataset_new.csv",
)

OUTPUT_DIR = Path(
    os.getenv(
        "BATCH_OUTPUT_DIR",
        "data/batch",
    )
)

SIMULATED_DAY_SECONDS = int(
    os.getenv(
        "SIMULATED_DAY_SECONDS",
        "60",
    )
)


def load_rows():
    with open(
        SOURCE_FILE,
        "r",
        encoding="utf-8",
    ) as file:
        reader = csv.DictReader(file)
        return list(reader)


def group_by_day(rows):
    days = {}

    for row in rows:
        timestamp = datetime.strptime(
            row["Timestamp"],
            "%Y-%m-%d %H:%M:%S",
        )

        day = timestamp.date()

        days.setdefault(day, []).append(row)

    return days


def write_daily_file(day, rows):
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_file = (
        OUTPUT_DIR
        / f"smart_grid_{day.isoformat()}.csv"
    )

    if output_file.exists():
        return output_file

    fieldnames = rows[0].keys()

    with open(
        output_file,
        "w",
        newline="",
        encoding="utf-8",
    ) as file:
        writer = csv.DictWriter(
            file,
            fieldnames=fieldnames,
        )

        writer.writeheader()
        writer.writerows(rows)

    return output_file


def main():
    rows = load_rows()
    days = group_by_day(rows)

    sorted_days = sorted(days.keys())

    print(
        f"Loaded {len(rows)} records "
        f"across {len(sorted_days)} simulated days."
    )

    for day in sorted_days:
        output_file = write_daily_file(
            day,
            days[day],
        )

        print(
            f"[BATCH SOURCE] "
            f"Completed simulated day {day} "
            f"→ {output_file}"
        )

        time.sleep(SIMULATED_DAY_SECONDS)


if __name__ == "__main__":
    main()