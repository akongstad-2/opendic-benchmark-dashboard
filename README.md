# OpenDIC Benchmark Dashboard

A dashboard for visualizing benchmark results from different databases.

## Installation

```bash
# Clone the repository
git clone <repository-url>
cd opendic-benchmark-dashboard

# Create a virtual environment (optional)
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install the package
pip install -e .
```

## Running the Dashboard

### Streamlit App (Recommended)

Run the Streamlit app with:

```bash
opendic-benchmark-streamlit
```

Or run directly with:

```bash
python -m streamlit run src/opendic_benchmark_dashboard/streamlit_app.py
```

### Original Plot Display

You can also run the original version (which just displays a plot without interactive features):

```bash
opendic-benchmark-dashboard
```

## Features

- Interactive dashboard built with Streamlit
- Select and visualize benchmark data from different databases
- Compare performance metrics across databases
- Filter by command types and granularity
- View raw data and statistics