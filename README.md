# pipecheck

A lightweight CLI tool to validate and profile ETL pipeline outputs against schema contracts.

---

## Installation

```bash
pip install pipecheck
```

Or install from source:

```bash
git clone https://github.com/yourname/pipecheck.git && cd pipecheck && pip install .
```

---

## Usage

Define a schema contract in YAML:

```yaml
# schema.yml
columns:
  - name: user_id
    type: integer
    nullable: false
  - name: email
    type: string
    nullable: false
  - name: signup_date
    type: date
```

Run `pipecheck` against your pipeline output:

```bash
pipecheck validate --data output.csv --schema schema.yml
```

Example output:

```
✔ user_id   — OK
✔ email     — OK
✗ signup_date — 3 null values found (nullable: false)

Summary: 2 passed, 1 failed
```

You can also profile a dataset without a schema:

```bash
pipecheck profile --data output.csv
```

---

## Options

| Flag | Description |
|------|-------------|
| `--data` | Path to the pipeline output file (CSV, Parquet) |
| `--schema` | Path to the schema contract YAML file |
| `--format` | Output format: `text` (default) or `json` |
| `--strict` | Exit with code 1 on any validation failure |

---

## License

This project is licensed under the [MIT License](LICENSE).