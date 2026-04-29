"""Export PipeCheckReport results to various file formats (JSON, CSV, Markdown)."""

from __future__ import annotations

import csv
import io
import json
from pathlib import Path
from typing import Union

from pipecheck.report import PipeCheckReport


def export_json(report: PipeCheckReport, path: Union[str, Path]) -> None:
    """Write the full report as a JSON file."""
    path = Path(path)
    path.write_text(report.to_json(), encoding="utf-8")


def export_csv(report: PipeCheckReport, path: Union[str, Path]) -> None:
    """Write validation errors and column profiles as a flat CSV file."""
    path = Path(path)
    rows = []

    for err in report.validation.errors:
        rows.append({"kind": "validation_error", "column": "", "detail": err})

    for col in report.profile.columns:
        rows.append({
            "kind": "profile",
            "column": col.name,
            "detail": (
                f"dtype={col.dtype}, null_count={col.null_count}, "
                f"null_pct={col.null_pct:.2f}, "
                f"unique={col.unique_count}"
            ),
        })

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=["kind", "column", "detail"])
    writer.writeheader()
    writer.writerows(rows)
    path.write_text(buf.getvalue(), encoding="utf-8")


def export_markdown(report: PipeCheckReport, path: Union[str, Path]) -> None:
    """Write a human-readable Markdown summary of the report."""
    path = Path(path)
    lines = [
        "# PipeCheck Report",
        "",
        "## Validation",
        f"- **Passed**: {report.validation.passed}",
        f"- **Errors**: {len(report.validation.errors)}",
    ]
    if report.validation.errors:
        lines.append("")
        lines.append("### Errors")
        for err in report.validation.errors:
            lines.append(f"- {err}")

    lines += [
        "",
        "## Profile",
        f"- **Rows**: {report.profile.row_count}",
        f"- **Columns**: {report.profile.column_count}",
        "",
        "### Column Details",
        "| Column | DType | Null Count | Null % | Unique |",
        "|--------|-------|-----------|--------|--------|" ,
    ]
    for col in report.profile.columns:
        lines.append(
            f"| {col.name} | {col.dtype} | {col.null_count} "
            f"| {col.null_pct:.1f} | {col.unique_count} |"
        )

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


FORMAT_HANDLERS = {
    "json": export_json,
    "csv": export_csv,
    "md": export_markdown,
    "markdown": export_markdown,
}


def export_report(report: PipeCheckReport, path: Union[str, Path], fmt: str = "json") -> None:
    """Dispatch export to the appropriate handler based on *fmt*."""
    fmt = fmt.lower()
    if fmt not in FORMAT_HANDLERS:
        raise ValueError(f"Unsupported export format '{fmt}'. Choose from: {list(FORMAT_HANDLERS)}.")
    FORMAT_HANDLERS[fmt](report, path)
