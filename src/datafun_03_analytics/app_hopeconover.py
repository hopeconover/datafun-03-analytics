"""app_hopeconover.py - Project script.

TODO: Read the examples carefully. Choose your data source of one of the provided types.
TODO: Create and implement a new Python file (module) in this folder following the associated example.
TODO: Your module should have:
- an appropriate name like yourname_type_pipeline.py (e.g., smith_csv_pipeline.py)
- start with a docstring similar to the examples
- add imports at the top.
- define an extract function (E that reads data from data/raw into memory)
- define a transform function (T that processes the extracted data)
- define a load function (L that writes output to data/processed)
- define a run_pipeline() function that calls E, T, L, and adds a new output file to data/processed/.
TODO: Import and call your new module run_pipeline function in this script.

Author: Hope Conover
Date: 2026-01

Practice key Python skills:
- pathlib for cross-platform paths
- logging (preferred over print)
- calling functions from modules
- clear ETL pipeline stages:
  E = Extract (read)
  T = Transform (process)
  V = Verify (check)
  L = Load (write results to data/processed

OBS:
  This is your file to practice and customize.
  Find the TODO comments, and as you complete each task, remove the TODO note.
"""


# === DECLARE IMPORTS (BRING IN FREE CODE) ===

# Imports from the Python standard library (free stuff that comes with Python).
import csv
import logging
from pathlib import Path
import statistics
from typing import Final

# REQ: imports from external packages must be listed in pyproject.toml dependencies
from datafun_toolkit.logger import get_logger, log_header

# === IMPORT LOCAL MODULE FUNCTIONS ===
# REQ: imports from other modules in this project must use full package path
# TODO: create and import your own data pipeline module here. See the example code.


# === CONFIGURE LOGGER ONCE PER MODULE ===

LOG: logging.Logger = get_logger("P03", level="DEBUG")

# === DECLARE GLOBAL VARIABLES ===

ROOT_DIR: Final[Path] = Path.cwd()
DATA_DIR: Final[Path] = ROOT_DIR / "data"
RAW_DIR: Final[Path] = DATA_DIR / "raw"
PROCESSED_DIR: Final[Path] = DATA_DIR / "processed"

# === DEFINE THE MAIN FUNCTION THAT WILL CALL OUR FUNCTIONS ===


def main() -> None:
    """Entry point: run four simple ETVL pipelines."""
    log_header(LOG, "Pipelines: Read, Process, Verify, Write (ETVL)")
    LOG.info("START main()")

    # TODO: call your imported data pipeline that reads from data/raw and writes to data/processed.

    LOG.info("END main()")


# === DEFINE ETL STEP FUNCTIONS ===
# === We add a VERIFY step to check data integrity ===


def extract_csv_scores(*, file_path: Path, column_name: str) -> list[float]:
    """E: Read CSV and extract one numeric column as floats.

    Args:
        file_path: Path to input CSV file.
        column_name: Name of the column to extract.

    Returns:
        List of float values from the specified column.
    """
    # Handle known possible error: no file at the path provided.
    if not file_path.exists():
        raise FileNotFoundError(f"Missing input file: {file_path}")

    scores: list[float] = []
    with file_path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        # Handle known possible error: missing expected column.
        if reader.fieldnames is None or column_name not in reader.fieldnames:
            raise KeyError(
                f"CSV missing expected column '{column_name}'. Found: {reader.fieldnames}"
            )

        for row in reader:
            raw_value = (row.get(column_name) or "").strip()
            if not raw_value:
                continue
            try:
                scores.append(float(raw_value))
            except ValueError:
                # Keep it simple: skip rows that do not convert cleanly.
                continue

    return scores


def transform_scores_to_stats(*, scores: list[float]) -> dict[str, float]:
    """T: Calculate basic statistics for a list of floats.

    Args:
        scores: List of float values.

    Returns:
        Dictionary with keys: count, min, max, mean, stdev.
    """
    if not scores:
        raise ValueError("No numeric values found for analysis.")

    return {
        "count": float(len(scores)),
        "min": min(scores),
        "max": max(scores),
        "mean": statistics.mean(scores),
        "stdev": statistics.stdev(scores) if len(scores) > 1 else 0.0,
    }


def verify_stats(*, stats: dict[str, float]) -> None:
    """V: Sanity-check the stats dictionary.

    Args:
        stats: Dictionary with statistics to verify.

    Raises:
        KeyError: If expected keys are missing.
        ValueError: If any stats values are invalid.

    Returns:
        None
    """
    required = {"count", "min", "max", "mean", "stdev"}
    missing = required - set(stats.keys())
    # Handle known possible error: missing required keys.
    if missing:
        raise KeyError(f"Missing stats keys: {sorted(missing)}")

    # Handle known possible error: count must be positive.
    if stats["count"] <= 0:
        raise ValueError("Count must be positive.")
    # Handle known possible error: min cannot be greater than max.
    if stats["min"] > stats["max"]:
        raise ValueError("Min cannot be greater than max.")


def load_stats_report(*, stats: dict[str, float], out_path: Path) -> None:
    """L: Write stats to a text file in data/processed.

    Args:
        stats: Dictionary with statistics to write.
        out_path: Path to output text file.

    Returns:
        None
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with out_path.open("w", encoding="utf-8") as f:
        f.write("CSV Ladder Score Statistics\n")
        f.write(f"Count: {int(stats['count'])}\n")
        f.write(f"Minimum: {stats['min']:.2f}\n")
        f.write(f"Maximum: {stats['max']:.2f}\n")
        f.write(f"Mean: {stats['mean']:.2f}\n")
        f.write(f"Standard Deviation: {stats['stdev']:.2f}\n")


# === DEFINE THE FULL PIPELINE FUNCTION ===


def run_csv_pipeline(
    *, raw_dir: Path, processed_dir: Path, logger: logging.Logger
) -> None:
    """Run the full ETVL pipeline.

    Args:
        raw_dir: Path to data/raw directory.
        processed_dir: Path to data/processed directory.
        logger: Logger for logging messages.

    Returns:
        None

    """
    logger.info("CSV: START")

    input_file = raw_dir / "2020_happiness.csv"
    output_file = processed_dir / "csv_ladder_score_stats.txt"

    # E
    scores = extract_csv_scores(file_path=input_file, column_name="Ladder score")

    # T
    stats = transform_scores_to_stats(scores=scores)

    # V
    verify_stats(stats=stats)

    # L
    load_stats_report(stats=stats, out_path=output_file)

    logger.info("CSV: wrote %s", output_file)
    logger.info("CSV: END")


# === CONDITIONAL EXECUTION GUARD ===

if __name__ == "__main__":
    main()
