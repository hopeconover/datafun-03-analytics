import csv
from pathlib import Path
import statistics


def run_crime_pipeline(*, raw_dir: Path, processed_dir: Path, logger):
    logger.info("CRIME PIPELINE: Starting")

    # E: EXTRACT (Read the data)
    input_file = raw_dir / "stl_crime_stats.csv"
    districts = []

    with Path.open(input_file, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                # We are pulling the 'District' column to do some math
                val = float(row["District"])
                districts.append(val)
            except (ValueError, KeyError):
                continue

    # T: TRANSFORM (Calculate stats)
    stats = {
        "count": len(districts),
        "mean_district": statistics.mean(districts) if districts else 0,
        "max_district": max(districts) if districts else 0,
    }

    # L: LOAD (Write the result)
    output_file = processed_dir / "crime_results.txt"
    processed_dir.mkdir(parents=True, exist_ok=True)

    with Path.open(output_file, mode="w") as f:
        f.write("STL Crime Analysis\n")
        f.write(f"Total Incidents: {stats['count']}\n")
        f.write(f"Average District Number: {stats['mean_district']:.2f}\n")

    logger.info(f"CRIME PIPELINE: Complete. Results in {output_file}")


def verify_crime_stats(*, stats: dict[str, float]) -> None:
    """V: Sanity-check the crime stats."""

    # 1. Check if we actually found data
    if stats["count"] == 0:
        raise ValueError("Verification Failed: No crime incidents were found!")

    # 2. Check for realistic district numbers (STL only has Districts 1-6)
    if stats["max_district"] > 6 or stats["min_district"] < 1:
        raise ValueError("Verification Failed: Found an invalid District number!")

    print("✅ Verification Successful: Stats look realistic.")
