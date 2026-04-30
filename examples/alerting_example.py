"""Example: evaluate a pipeline run and emit alerts based on thresholds."""

import pandas as pd

from pipecheck.alerting import evaluate_alerts
from pipecheck.profiler import profile
from pipecheck.schema import PipelineSchema, ColumnSchema
from pipecheck.scorer import QualityScore
from pipecheck.threshold import ThresholdConfig
from pipecheck.validator import validate
from pipecheck.watcher import detect_drift

# ---------------------------------------------------------------------------
# 1. Build a sample dataframe and schema
# ---------------------------------------------------------------------------
df = pd.DataFrame(
    {
        "order_id": [1, 2, 3, 4, 5],
        "customer": ["alice", "bob", None, "dana", "eve"],
        "amount": [10.5, 20.0, 5.5, 8.0, 15.0],
    }
)

schema = PipelineSchema(
    name="orders",
    version="1.0",
    columns=[
        ColumnSchema(name="order_id", dtype="int64", nullable=False),
        ColumnSchema(name="customer", dtype="object", nullable=True),
        ColumnSchema(name="amount", dtype="float64", nullable=False),
    ],
)

# ---------------------------------------------------------------------------
# 2. Validate and detect drift against a baseline schema
# ---------------------------------------------------------------------------
validation_result = validate(df, schema)

baseline_schema = PipelineSchema(
    name="orders",
    version="0.9",
    columns=[
        ColumnSchema(name="order_id", dtype="int64", nullable=False),
        ColumnSchema(name="customer", dtype="object", nullable=False),  # nullable changed
        ColumnSchema(name="amount", dtype="float64", nullable=False),
    ],
)
drift_report = detect_drift(df, baseline_schema)

# ---------------------------------------------------------------------------
# 3. Score the run
# ---------------------------------------------------------------------------
score = QualityScore.compute(
    validation_result=validation_result,
    drift_report=drift_report,
)
print(f"Quality score : {score.overall:.2f}  (grade: {score.grade})")

# ---------------------------------------------------------------------------
# 4. Evaluate alerts
# ---------------------------------------------------------------------------
config = ThresholdConfig(
    min_quality_score=0.85,
    max_drift_columns=0,
)
alert_report = evaluate_alerts(score, config)

print("\n--- Alert Summary ---")
print(alert_report.summary())
print("\nHas errors:", alert_report.has_errors)
