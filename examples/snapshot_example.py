"""Example: take, save, load, and diff two snapshots."""

from pathlib import Path

import pandas as pd

from pipecheck.snapshotter import take_snapshot, save_snapshot, load_snapshot, list_snapshots
from pipecheck.snapshot_diff import diff_snapshots

SNAPSHOT_DIR = Path(".pipecheck_snapshots_example")

# --- Simulate two pipeline runs ---

df_v1 = pd.DataFrame(
    {
        "order_id": [1, 2, 3, 4],
        "amount": [10.0, 20.0, 15.0, 5.0],
        "status": ["paid", "paid", "pending", "paid"],
    }
)

df_v2 = pd.DataFrame(
    {
        "order_id": [1, 2, 3, 4, 5],
        "amount": [10.0, 20.0, 15.0, 5.0, 99.0],
        "status": ["paid", "paid", "pending", "paid", "refunded"],
        "region": ["US", "EU", "US", "EU", "US"],  # new column
    }
)

# --- Take and save snapshots ---

snap1 = take_snapshot(df_v1, run_id="run_2024_01", tags={"env": "prod"})
snap2 = take_snapshot(df_v2, run_id="run_2024_02", tags={"env": "prod"})

path1 = save_snapshot(snap1, directory=SNAPSHOT_DIR)
path2 = save_snapshot(snap2, directory=SNAPSHOT_DIR)

print(f"Saved snapshots:\n  {path1}\n  {path2}")

# --- List available snapshots ---

available = list_snapshots(SNAPSHOT_DIR)
print(f"\nAvailable snapshots: {available}")

# --- Load and diff ---

loaded1 = load_snapshot("run_2024_01", directory=SNAPSHOT_DIR)
loaded2 = load_snapshot("run_2024_02", directory=SNAPSHOT_DIR)

diff = diff_snapshots(loaded1, loaded2)

print(f"\nHas changes: {diff.has_changes}")
print()
print(diff.summary())
