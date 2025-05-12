# OpenDIC Benchmark Dashboard

A dashboard for visualizing benchmark results from different databases.

## Installation

```bash
# Install uv
# Create a virtual environment (optional)
uv venv
source .venv/bin/activate

# Install the package
uv sync
```

## Running the Dashboard

### Streamlit App (Recommended)

Run the Streamlit app with:

```bash
# install taskfile
task run
```

Or run directly with:

```bash
uv run python -m streamlit run streamlit_app.py
```

## Features

- Sections: TLDR, Standard, Opendict, Opendict batch
- Subsections: Sqlite, duckdb, snowflake, postgres, opendict-file, opendict-file-cache, opendict-cloud-cache, opendict-file-batch, opendict-file-cache-batch, opendict-cloud-cache-batch
- Select and visualize benchmark data from different databases
- Compare performance metrics across databases
- Filter by command types and granularity
- View raw data and statistics
