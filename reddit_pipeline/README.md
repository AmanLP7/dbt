# Reddit Data Pipeline

This dbt project is designed to transform and model Reddit data on Databricks.

## Architecture & Flow
1. **Extraction**: Retrieve raw Reddit data (submissions, comments, subreddits) via the Reddit API/Pushshift or upstream staging datasets.
2. **Load**: Load the raw JSON/CSV data into Databricks Delta tables.
3. **Transformation (dbt)**: Model and clean raw data into staging, intermediate, and marts layers.

## Project Structure
- `models/staging/`: Raw source data cleaning, renaming, and type casting.
- `models/intermediate/`: Complex joins, aggregations, and business logic.
- `models/marts/`: Final analytics-ready tables for consumption (e.g. sentiment analysis, post metrics, user activity).

## Quick Start

### Prerequisites
- Python 3.10+
- A virtual environment with dbt-databricks installed:
  ```bash
  pip install dbt-databricks
  ```

### Profile Setup
Ensure you have your connection configuration defined in `~/.dbt/profiles.yml`:
```yaml
reddit_pipeline:
  outputs:
    dev:
      type: databricks
      schema: [your_target_schema]
      host: [your_databricks_host]
      http_path: [your_databricks_http_path]
      token: [your_databricks_token]
      threads: 4
  target: dev
```

### Running the Pipeline
Check connection:
```bash
dbt debug
```

Run models:
```bash
dbt run
```

Run tests:
```bash
dbt test
```
