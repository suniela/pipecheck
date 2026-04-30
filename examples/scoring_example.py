"""Example: compute a quality score for a pipeline run."""
import json
import pandas as pd

from pipecheck.schema import PipelineSchema, ColumnSchema
from pipecheck.validator import validate
from pipecheck.watcher import watch
from pipecheck.scorer import score_pipeline

# ------------------------------------------------------------------
# 1. Define a schema
# ------------------------------------------------------------------
schema = PipelineSchema(
    name="orders",
    columns=[
        ColumnSchema(name="order_id", dtype="int64", nullable=False),
        ColumnSchema(name="customer", dtype="object", nullable=False),
        ColumnSchema(name="amount", dtype="float64", nullable=True),
    ],
)

# ------------------------------------------------------------------
# 2. Simulate a DataFrame with a minor issue
# ------------------------------------------------------------------
df = pd.DataFrame(
    {
        "order_id": [1, 2, 3],
        "customer": ["Alice", "Bob", None],   # nullable=False violation
        "amount": [9.99, 14.50, 7.00],
    }
)

# ------------------------------------------------------------------
# 3. Validate
# ------------------------------------------------------------------
validation_result = validate(df, schema)
print("Validation passed:", validation_result.passed)
for err in validation_result.errors:
    print(" -", err)

# ------------------------------------------------------------------
# 4. Drift detection (compare against a reference DataFrame)
# ------------------------------------------------------------------
reference_df = pd.DataFrame(
    {
        "order_id": [10, 11],
        "customer": ["Carol", "Dave"],
        "amount": [5.00, 22.00],
    }
)
drift_report = watch(reference_df, df, schema)
print("\nDrift detected:", drift_report.has_drift)

# ------------------------------------------------------------------
# 5. Score
# ------------------------------------------------------------------
score = score_pipeline(validation_result, drift_report)
print("\nQuality Score:")
print(json.dumps(score.to_dict(), indent=2))
