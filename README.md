# OpenDict Benchmark Dashboard

This dashboard was developed at ITU as part of a the Master thesis: _OpenDict: An Approach to Open Management of All Metadata Objects_.

**Purpose**: The main purpose of the dashboard is to visualize the results our metadata operation performance experiment.

**Authors**: Andreas Kongstad & Carl Bruun

![alt text](<assets/Screenshot 2025-06-02 at 03.01.38.png>)

A dashboard for visualizing benchmark results from different databases.

## Overview

- **results**: Parquet files containing exported benchmark results.  
- **src/storage_data.py**: Storage results from the performance experiment.  
- **streamlit_app.py**: Code for the Streamlit app that creates the dashboard.  
- **taskfile.toml**: Task definitions.

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
