"""Command-line interface for pipecheck.

Provides the `pipecheck` CLI entry point for validating and profiling
ETL pipeline outputs against schema contracts.

Usage:
    pipecheck run <data_file> <schema_file> [--profile] [--output <file>]
    pipecheck validate <data_file> <schema_file>
    pipecheck profile <data_file>
"""

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

from pipecheck.report import build_report
from pipecheck.schema import load_schema
from pipecheck.validator import validate
from pipecheck.profiler import profile


def _load_dataframe(path: str) -> pd.DataFrame:
    """Load a CSV or JSON file into a DataFrame based on file extension."""
    file_path = Path(path)
    if not file_path.exists():
        print(f"[error] Data file not found: {path}", file=sys.stderr)
        sys.exit(1)

    suffix = file_path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(file_path)
    elif suffix == ".json":
        return pd.read_json(file_path)
    elif suffix == ".parquet":
        return pd.read_parquet(file_path)
    else:
        print(
            f"[error] Unsupported file format '{suffix}'. Use .csv, .json, or .parquet.",
            file=sys.stderr,
        )
        sys.exit(1)


def cmd_run(args: argparse.Namespace) -> int:
    """Run full validation and optional profiling, then print a summary report."""
    df = _load_dataframe(args.data_file)
    schema = load_schema(args.schema_file)

    include_profile = getattr(args, "profile", False)
    report = build_report(df, schema, include_profile=include_profile)

    report.print_summary()

    if args.output:
        output_path = Path(args.output)
        output_path.write_text(report.to_json(indent=2))
        print(f"\nReport written to {args.output}")

    # Exit with non-zero code if validation failed
    return 0 if report.validation.passed else 1


def cmd_validate(args: argparse.Namespace) -> int:
    """Run validation only and print results."""
    df = _load_dataframe(args.data_file)
    schema = load_schema(args.schema_file)

    result = validate(df, schema)
    print(result.summary())

    return 0 if result.passed else 1


def cmd_profile(args: argparse.Namespace) -> int:
    """Profile a data file and print column statistics."""
    df = _load_dataframe(args.data_file)
    data_profile = profile(df)

    output = json.dumps(data_profile.as_dict(), indent=2, default=str)
    print(output)

    if args.output:
        output_path = Path(args.output)
        output_path.write_text(output)
        print(f"\nProfile written to {args.output}", file=sys.stderr)

    return 0


def build_parser() -> argparse.ArgumentParser:
    """Build and return the argument parser for the pipecheck CLI."""
    parser = argparse.ArgumentParser(
        prog="pipecheck",
        description="Validate and profile ETL pipeline outputs against schema contracts.",
    )
    subparsers = parser.add_subparsers(dest="command", metavar="<command>")
    subparsers.required = True

    # --- run command ---
    run_parser = subparsers.add_parser(
        "run", help="Validate data and optionally profile it."
    )
    run_parser.add_argument("data_file", help="Path to the data file (.csv, .json, .parquet)")
    run_parser.add_argument("schema_file", help="Path to the schema JSON file")
    run_parser.add_argument(
        "--profile", action="store_true", help="Include column profiling in the report"
    )
    run_parser.add_argument(
        "--output", metavar="FILE", help="Write the full report to a JSON file"
    )

    # --- validate command ---
    val_parser = subparsers.add_parser("validate", help="Validate data against a schema.")
    val_parser.add_argument("data_file", help="Path to the data file (.csv, .json, .parquet)")
    val_parser.add_argument("schema_file", help="Path to the schema JSON file")

    # --- profile command ---
    prof_parser = subparsers.add_parser("profile", help="Profile a data file.")
    prof_parser.add_argument("data_file", help="Path to the data file (.csv, .json, .parquet)")
    prof_parser.add_argument(
        "--output", metavar="FILE", help="Write the profile to a JSON file"
    )

    return parser


def main() -> None:
    """Main entry point for the pipecheck CLI."""
    parser = build_parser()
    args = parser.parse_args()

    dispatch = {
        "run": cmd_run,
        "validate": cmd_validate,
        "profile": cmd_profile,
    }

    handler = dispatch[args.command]
    exit_code = handler(args)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
