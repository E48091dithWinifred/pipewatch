# pipewatch

A lightweight CLI tool for monitoring ETL pipeline health with configurable alerting thresholds.

---

## Installation

```bash
pip install pipewatch
```

Or install from source:

```bash
git clone https://github.com/yourusername/pipewatch.git && cd pipewatch && pip install -e .
```

---

## Usage

Run a health check against a pipeline config file:

```bash
pipewatch check --config pipeline.yaml
```

Example `pipeline.yaml`:

```yaml
pipelines:
  - name: daily_sales_etl
    schedule: "0 6 * * *"
    thresholds:
      max_duration_minutes: 30
      min_rows_processed: 1000
      error_rate_percent: 5
```

Watch continuously and receive alerts when thresholds are breached:

```bash
pipewatch watch --config pipeline.yaml --interval 60 --alert slack
```

View status of all monitored pipelines:

```bash
pipewatch status
```

### Options

| Flag | Description |
|------|-------------|
| `--config` | Path to pipeline configuration file |
| `--interval` | Polling interval in seconds (default: 30) |
| `--alert` | Alert channel: `slack`, `email`, or `log` |
| `--verbose` | Enable verbose output |

---

## License

This project is licensed under the [MIT License](LICENSE).