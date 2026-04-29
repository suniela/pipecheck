"""Example script: run pipecheck on the orders dataset and export results.

Usage:
    python examples/export_report.py

Outputs three files in examples/output/:
    report.json, report.csv, report.md
"""

from pathlib import Path

import pandas as pd

from pipecheck.exporter import export_report
from pipecheck.profiler import profile
from pipecheck.report import build_report
from pipecheck.schema import load_schema
from pipecheck.validator import validate

EXAMPLES_DIR = Path(__file__).parent
OUTPUT_DIR = EXAMPLES_DIR / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

# ── Load schema ──────────────────────────────────────────────────────────────
schema = load_schema(EXAMPLES_DIR / "orders_schema.json")

# ── Build a sample DataFrame matching the orders schema ──────────────────────
df = pd.DataFrame({
    "order_id":    [1001, 1002, 1003, 1004],
    "customer_id": [1, 2, 3, 4],
    "status":      ["shipped", "pending", "cancelled", "shipped"],
    "amount":      [250.00, 89.99, 0.00, 412.50],
    "created_at":  [
        "2024-01-15", "2024-01-16", "2024-01-17", "2024-01-18"
    ],
})

# ── Validate & profile ────────────────────────────────────────────────────────
validation_result = validate(df, schema)
profile_result = profile(df)

# ── Build unified report ──────────────────────────────────────────────────────
report = build_report(validation_result, profile_result)
report.print_summary()

# ── Export in all supported formats ──────────────────────────────────────────
for fmt in ("json", "csv", "md"):
    out_path = OUTPUT_DIR / f"report.{fmt}"
    export_report(report, out_path, fmt=fmt)
    print(f"Exported {fmt.upper()} → {out_path}")
