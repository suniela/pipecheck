"""Tests for pipecheck.exporter — JSON, CSV, and Markdown export."""

import json
import textwrap
from pathlib import Path

import pandas as pd
import pytest

from pipecheck.profiler import profile
from pipecheck.report import build_report
from pipecheck.schema import ColumnSchema, PipelineSchema
from pipecheck.validator import validate
from pipecheck.exporter import export_report


@pytest.fixture()
def sample_report(tmp_path):
    df = pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["alice", "bob", None],
        "amount": [10.0, 20.0, 30.0],
    })
    schema = PipelineSchema(
        name="test",
        columns=[
            ColumnSchema(name="id", dtype="int64", nullable=False),
            ColumnSchema(name="name", dtype="object", nullable=True),
            ColumnSchema(name="amount", dtype="float64", nullable=False),
        ],
    )
    validation = validate(df, schema)
    prof = profile(df)
    return build_report(validation, prof)


class TestExportJson:
    def test_creates_file(self, sample_report, tmp_path):
        out = tmp_path / "report.json"
        export_report(sample_report, out, fmt="json")
        assert out.exists()

    def test_valid_json_content(self, sample_report, tmp_path):
        out = tmp_path / "report.json"
        export_report(sample_report, out, fmt="json")
        data = json.loads(out.read_text())
        assert "validation" in data
        assert "profile" in data


class TestExportCsv:
    def test_creates_file(self, sample_report, tmp_path):
        out = tmp_path / "report.csv"
        export_report(sample_report, out, fmt="csv")
        assert out.exists()

    def test_csv_has_header_and_rows(self, sample_report, tmp_path):
        out = tmp_path / "report.csv"
        export_report(sample_report, out, fmt="csv")
        lines = out.read_text().splitlines()
        assert lines[0] == "kind,column,detail"
        assert len(lines) > 1

    def test_profile_rows_present(self, sample_report, tmp_path):
        out = tmp_path / "report.csv"
        export_report(sample_report, out, fmt="csv")
        content = out.read_text()
        assert "profile" in content
        assert "id" in content


class TestExportMarkdown:
    def test_creates_file(self, sample_report, tmp_path):
        out = tmp_path / "report.md"
        export_report(sample_report, out, fmt="md")
        assert out.exists()

    def test_contains_sections(self, sample_report, tmp_path):
        out = tmp_path / "report.md"
        export_report(sample_report, out, fmt="md")
        content = out.read_text()
        assert "# PipeCheck Report" in content
        assert "## Validation" in content
        assert "## Profile" in content

    def test_column_table_present(self, sample_report, tmp_path):
        out = tmp_path / "report.md"
        export_report(sample_report, out, fmt="md")
        content = out.read_text()
        assert "| Column |" in content


class TestExportDispatch:
    def test_unknown_format_raises(self, sample_report, tmp_path):
        with pytest.raises(ValueError, match="Unsupported export format"):
            export_report(sample_report, tmp_path / "out.xyz", fmt="xyz")

    def test_markdown_alias(self, sample_report, tmp_path):
        out = tmp_path / "report.md"
        export_report(sample_report, out, fmt="markdown")
        assert out.exists()
