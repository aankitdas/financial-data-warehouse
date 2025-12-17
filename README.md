# Financial Data Warehouse
A data warehouse I built with Snowflake, dbt, and Python. Ingests financial data, transforms it into a star schema, and runs automated tests on every commit via GitHub Actions.
Architecture
Raw Data → Python ETL → Snowflake RAW → dbt Staging → dbt Mart → Analytics Ready
What's Inside

Python ETL pipeline (scripts/ingest_financial_data.py): Fetches data, validates it, loads to Snowflake
dbt transformations (3 models): Cleans data, builds dimensional fact/dimension tables
Automated tests: Validates data uniqueness, nullness, relationships
CI/CD: GitHub Actions runs dbt on every push, generates docs

Setup
```bash
# Clone the repository
git clone https://github.com/aankitdas/financial_data_warehouse.git
cd financial_data_warehouse

# Install dependencies
uv sync

# Create .env with Snowflake credentials
cat > .env << EOF
SNOWFLAKE_ACCOUNT=your_account_id
SNOWFLAKE_USER=your_email
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_DATABASE=FINANCE_WAREHOUSE
SNOWFLAKE_SCHEMA=RAW
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
EOF

# Run ETL
uv run python scripts/ingest_financial_data.py

# Run dbt
cd financial_dbt
uv run dbt run
uv run dbt test

# View docs
uv run dbt docs serve
```
## Tech Stack

Warehouse: Snowflake
Transformation: dbt
ETL: Python + uv
CI/CD: GitHub Actions
Testing: dbt tests, SQL validation

## Project Structure
```
├── scripts/
│   └── ingest_financial_data.py
├── financial_dbt/
│   ├── models/staging/
│   ├── models/marts/
│   └── schema.yml
├── .github/workflows/dbt_ci.yml
├── pyproject.toml
└── uv.lock
```
## Key Decisions

Used uv instead of pip for faster, more reliable dependency management
Quoted all column names in SQL to avoid Snowflake case-sensitivity issues
Dimensional modeling with surrogate keys for scalability
dbt tests for data quality (uniqueness, not null, relationships)
GitHub Actions for automated CI/CD on every push
