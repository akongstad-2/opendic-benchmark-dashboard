import os

import pandas as pd
import plotly.express as px
import streamlit as st

# Set page title and layout
st.set_page_config(page_title="OpenDIC Benchmark Dashboard", layout="wide")

# Add title and description
st.title("OpenDIC Benchmark Dashboard")
st.write("Visualize and compare benchmark results for different databases")

# Create tabs for switching between different dataset categories
st.sidebar.header("Dashboard Controls")
sidebar_category = st.sidebar.radio("Select Dataset Category", options=["Standard", "Opendic", "Opendic(Batch)"])


def create_dashboard(data_dir):
    data_files = [f for f in os.listdir(data_dir) if f.endswith(".parquet")]
    database_options = sorted([os.path.splitext(f)[0] for f in data_files])

    # Sidebar for controls
    selected_db = st.sidebar.selectbox("Select Experiment", options=database_options, index=2 if len(database_options) > 2 else 0)

    # Load data
    data_file = f"{data_dir}{selected_db}.parquet"
    data_df = pd.read_parquet(data_file, engine="pyarrow")

    # Display raw data in expandable section
    with st.expander("View Raw Data"):
        st.dataframe(data_df, use_container_width=True)

    if sidebar_category == "Standard":
        standard_create_dashboard(data_df, selected_db=selected_db)
    elif sidebar_category == "Opendic":
        opendic_dashboard(data_df, selected_db=selected_db)
    elif sidebar_category == "Opendic(Batch)":
        opendic_batch_dashboard(data_df, selected_db=selected_db)


def standard_create_dashboard(data_df, selected_db):
    # Filter for 'CREATE' commands and average runtimes
    create_summary_df = (
        data_df[data_df["ddl_command"] == "CREATE"]
        .groupby(["ddl_command", "target_object", "granularity"], as_index=False)
        .agg(avg_runtime=(("query_runtime"), "mean"))
    )

    # group by these chunk IDs and compute the average for each chunk
    create_summary_chunked_df = chunked_avg_runtime(
        create_summary_df,
        chunk_size=20,
    )

    plot_create(create_summary_chunked_df, experiment_name=selected_db)


def opendic_dashboard(data_df, selected_db):
    # Filter for 'CREATE' commands and average runtimes
    create_summary = (
        data_df[data_df["ddl_command"] == "CREATE"]
        .groupby(["ddl_command", "target_object", "granularity"], as_index=False)
        .agg(avg_runtime=(("query_runtime"), "mean"))
    )

    # group by these chunk IDs and compute the average for each chunk
    create_summary_chunked_df = chunked_avg_runtime(
        create_summary,
        chunk_size=20,
    )

    plot_create(create_summary_chunked_df, selected_db)


def opendic_batch_dashboard(data_df, selected_db: str):
    # Filter for 'CREATE' commands and average runtimes
    create_summary_df = (
        data_df[data_df["ddl_command"] == "CREATE"]
        .groupby(["ddl_command", "target_object", "granularity"], as_index=False)
        .agg(query_runtime=("query_runtime", "sum"))
    )

    # Add y-axis type control to sidebar
    y_axis_type = st.sidebar.selectbox("Y-axis scale", options=["Linear", "Log"], index=0)

    # Create visualization for CREATE commands
    st.subheader(f"Average CREATE Query Runtime by Object & Granularity for {selected_db.capitalize()}")
    fig = px.line(
        create_summary_df,
        x="granularity",
        y="query_runtime",
        color="target_object",
        labels={"target_object": "Target Object", "query_runtime": "Runtime (s)", "granularity": "Granularity"},
        log_y=(y_axis_type == "Log"),  # Apply log scale if selected
    )

    fig.update_layout(xaxis_tickangle=-45, legend_title="Object Type", template="plotly_white", yaxis=dict(title="Runtime (s)"))
    st.plotly_chart(fig, use_container_width=True)


def chunked_avg_runtime(data_df, chunk_size=20):
    # Create chunked averages (each row represents the average of 20 rows)
    # assign chunk IDs to each row
    create_summary = data_df.reset_index(drop=True)
    create_summary["chunk_id"] = create_summary.index // 20

    # group by these chunk IDs and compute the average for each chunk
    return create_summary.groupby(["chunk_id", "target_object"], as_index=False).agg(
        avg_runtime=("avg_runtime", "mean"),
        granularity=("granularity", lambda x: x.iloc[0]),  # Take the first granularity value from each chunk
    )


def plot_create(data_df, experiment_name):
    # Add y-axis type control to sidebar
    y_axis_type = st.sidebar.selectbox("Y-axis scale", options=["Linear", "Log"], index=0)

    # Create visualization for CREATE commands
    st.subheader(f"Average CREATE Query Runtime by Object & Granularity for {experiment_name.capitalize()}")
    fig = px.line(
        data_df,
        x="granularity",
        y="avg_runtime",
        color="target_object",
        labels={"target_object": "Target Object", "avg_runtime": "Avg. Runtime (s)", "granularity": "Granularity"},
        log_y=(y_axis_type == "Log"),  # Apply log scale if selected
    )

    fig.update_layout(
        xaxis_tickangle=-45,
        legend_title="Object Type",
        template="plotly_white",
        yaxis=dict(title="Avg. Runtime (s)")
        if y_axis_type == "Log"
        else dict(
            title="Avg. Runtime (s)",
            range=[0, data_df["avg_runtime"].quantile(0.999)],  # Remove blantant outliers
        ),
    )
    st.plotly_chart(fig, use_container_width=True)


if __name__ == "__main__":
    category_map = {"Standard": "data/standard/", "Opendic": "data/opendic/", "Opendic(Batch)": "data/opendic_batch/"}
    create_dashboard(category_map[sidebar_category])
