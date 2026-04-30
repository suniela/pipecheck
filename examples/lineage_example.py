"""Example demonstrating lineage tracking with pipecheck."""
import pandas as pd

from pipecheck.schema import load_schema
from pipecheck.validator import validate
from pipecheck.profiler import profile
from pipecheck.scorer import compute_score
from pipecheck.watcher import detect_drift
from pipecheck.lineage import LineageLog, build_lineage_entry

# ---------------------------------------------------------------------------
# Simulate two pipeline runs and record lineage for each
# ---------------------------------------------------------------------------

log = LineageLog()

schema = load_schema("examples/orders_schema.json")

# --- Run 1 ---
df1 = pd.DataFrame({
    "order_id": [1, 2, 3],
    "customer_id": [10, 20, 30],
    "amount": [99.9, 149.5, 200.0],
    "status": ["shipped", "pending", "delivered"],
    "created_at": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
})

result1 = validate(df1, schema)
score1 = compute_score(result1)

entry1 = build_lineage_entry(
    run_id="run-2024-001",
    source="orders_2024_01.csv",
    schema_name=schema.name,
    row_count=len(df1),
    column_count=len(df1.columns),
    passed=result1.passed,
    quality_score=score1.score,
    tags=["january", "batch"],
)
log.add(entry1)

# --- Run 2 (with a drift scenario) ---
df2 = pd.DataFrame({
    "order_id": [4, 5],
    "customer_id": [40, 50],
    "amount": [None, 75.0],       # null introduced
    "status": ["refunded", "shipped"],
    "created_at": pd.to_datetime(["2024-02-01", "2024-02-02"]),
})

result2 = validate(df2, schema)
score2 = compute_score(result2)
drift = detect_drift(df2, schema)

entry2 = build_lineage_entry(
    run_id="run-2024-002",
    source="orders_2024_02.csv",
    schema_name=schema.name,
    row_count=len(df2),
    column_count=len(df2.columns),
    passed=result2.passed,
    quality_score=score2.score,
    drift_detected=drift.has_drift,
    tags=["february", "batch"],
)
log.add(entry2)

# --- Summary ---
print("=== Lineage Log ===")
for entry in log.entries:
    status = "PASS" if entry.passed else "FAIL"
    drift_flag = " [DRIFT]" if entry.drift_detected else ""
    print(f"  [{entry.run_id}] {status}{drift_flag} | rows={entry.row_count} | score={entry.quality_score:.2f}")

print(f"\nTotal runs recorded : {len(log.entries)}")
print(f"Latest run          : {log.latest().run_id}")
print(f"\nJSON snapshot:\n{log.to_json()}")
