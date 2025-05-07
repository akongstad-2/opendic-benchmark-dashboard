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
    # Overview dashboard
    # Filter for 'CREATE' commands and average runtimes
    create_df = (
        data_df[data_df["ddl_command"] == "CREATE"]
        .groupby(["ddl_command", "target_object", "granularity"], as_index=False)
        .agg(avg_runtime=(("query_runtime"), "mean"))
    )
    create_summary_df = chunked_avg_runtime(
        create_df,
        chunk_size=20,
    )
    alter_summary_df = (
        data_df[data_df["ddl_command"] == "ALTER"]
        .groupby(["ddl_command", "target_object", "granularity"], as_index=False)
        .agg(avg_runtime=(("query_runtime"), "mean"))
    )
    comment_summary_df = (
        data_df[data_df["ddl_command"] == "COMMENT"]
        .groupby(["ddl_command", "target_object", "granularity"], as_index=False)
        .agg(avg_runtime=(("query_runtime"), "mean"))
    )
    show_summary_df = (
        data_df[data_df["ddl_command"] == "SHOW"]
        .groupby(["ddl_command", "target_object", "granularity"], as_index=False)
        .agg(avg_runtime=(("query_runtime"), "mean"))
    )
    # Combine all summaries
    small_create_df = chunked_avg_runtime(create_df, chunk_size=250)
    all_df = pd.concat([small_create_df, alter_summary_df, comment_summary_df, show_summary_df])

    # Create a summary_df that averages across target_object
    summary_df = all_df.groupby(["ddl_command", "granularity"], as_index=False).agg(avg_runtime=("avg_runtime", "mean"))

    # Add y-axis type control to sidebar
    y_axis_type = st.sidebar.selectbox("Y-axis scale", options=["Linear", "Log"], index=0)

    plot_summary(summary_df, experiment_name=selected_db, y_axis_type=y_axis_type)
    plot_create(create_summary_df, experiment_name=selected_db, y_axis_type=y_axis_type)
    plot_ddl(alter_summary_df, "ALTER", experiment_name=selected_db, y_axis_type=y_axis_type)
    plot_ddl(comment_summary_df, "COMMENT", experiment_name=selected_db, y_axis_type=y_axis_type)
    plot_ddl(show_summary_df, "SHOW", experiment_name=selected_db, y_axis_type=y_axis_type)


def opendic_dashboard(data_df, selected_db):
    # Filter for 'CREATE' commands and average runtimes
    create_df = (
        data_df[data_df["ddl_command"] == "CREATE"]
        .groupby(["ddl_command", "target_object", "granularity"], as_index=False)
        .agg(avg_runtime=(("query_runtime"), "mean"))
    )
    create_summary_df = chunked_avg_runtime(
        create_df,
        chunk_size=20,
    )
    alter_summary_df = (
        data_df[data_df["ddl_command"] == "ALTER"]
        .groupby(["ddl_command", "target_object", "granularity"], as_index=False)
        .agg(avg_runtime=(("query_runtime"), "mean"))
    )
    comment_summary_df = (
        data_df[data_df["ddl_command"] == "COMMENT"]
        .groupby(["ddl_command", "target_object", "granularity"], as_index=False)
        .agg(avg_runtime=(("query_runtime"), "mean"))
    )
    show_summary_df = (
        data_df[data_df["ddl_command"] == "SHOW"]
        .groupby(["ddl_command", "target_object", "granularity"], as_index=False)
        .agg(avg_runtime=(("query_runtime"), "mean"))
    )
    small_create_df = chunked_avg_runtime(create_df, chunk_size=250)
    summary_df = pd.concat([small_create_df, alter_summary_df, comment_summary_df, show_summary_df])

    # Add y-axis type control to sidebar
    y_axis_type = st.sidebar.selectbox("Y-axis scale", options=["Linear", "Log"], index=0)

    # Plot the summary dataframes
    plot_summary(summary_df, experiment_name=selected_db, y_axis_type=y_axis_type)
    plot_create(create_summary_df, experiment_name=selected_db, y_axis_type=y_axis_type)
    plot_ddl(alter_summary_df, "ALTER", experiment_name=selected_db, y_axis_type=y_axis_type)
    plot_ddl(comment_summary_df, "COMMENT", experiment_name=selected_db, y_axis_type=y_axis_type)
    plot_ddl(show_summary_df, "SHOW", experiment_name=selected_db, y_axis_type=y_axis_type)



def opendic_batch_dashboard(data_df, selected_db: str):
    # Filter for 'CREATE' commands and average runtimes
    create_summary_df = (
        data_df[data_df["ddl_command"] == "CREATE"]
        .groupby(["ddl_command", "target_object", "granularity"], as_index=False)
        .agg(avg_runtime=("query_runtime", "sum"))
    )
    alter_summary_df = (
        data_df[data_df["ddl_command"] == "ALTER"]
        .groupby(["ddl_command", "target_object", "granularity"], as_index=False)
        .agg(avg_runtime=(("query_runtime"), "mean"))
    )
    comment_summary_df = (
        data_df[data_df["ddl_command"] == "COMMENT"]
        .groupby(["ddl_command", "target_object", "granularity"], as_index=False)
        .agg(avg_runtime=(("query_runtime"), "mean"))
    )
    show_summary_df = (
        data_df[data_df["ddl_command"] == "SHOW"]
        .groupby(["ddl_command", "target_object", "granularity"], as_index=False)
        .agg(avg_runtime=(("query_runtime"), "mean"))
    )
    summary_df = pd.concat([create_summary_df, alter_summary_df, comment_summary_df, show_summary_df])

    # Add y-axis type control to sidebar
    y_axis_type = st.sidebar.selectbox("Y-axis scale", options=["Linear", "Log"], index=0)

    plot_summary(summary_df, experiment_name=selected_db, y_axis_type=y_axis_type)
    plot_create(create_summary_df, experiment_name=selected_db, y_axis_type=y_axis_type)
    plot_ddl(alter_summary_df, "ALTER", experiment_name=selected_db, y_axis_type=y_axis_type)
    plot_ddl(comment_summary_df, "COMMENT", experiment_name=selected_db, y_axis_type=y_axis_type)
    plot_ddl(show_summary_df, "SHOW", experiment_name=selected_db, y_axis_type=y_axis_type)




def chunked_avg_runtime(data_df, chunk_size=20):
    # Create chunked averages (each row represents the average of 20 rows)
    # assign chunk IDs to each row
    create_summary = data_df.reset_index(drop=True)
    create_summary["chunk_id"] = create_summary.index // chunk_size

    # group by these chunk IDs and compute the average for each chunk
    return create_summary.groupby(["ddl_command", "chunk_id", "target_object"], as_index=False).agg(
        avg_runtime=("avg_runtime", "mean"),
        granularity=("granularity", lambda x: x.iloc[0]),  # Take the first granularity value from each chunk
    )


def plot_summary(data_df, experiment_name, y_axis_type):
    st.subheader(f"Average Query Runtime by DDL type, Object & Granularity for {experiment_name.capitalize()}")
    with st.expander("Query Data"):
        st.dataframe(data_df, use_container_width=True)

    fig = px.line(
        data_df,
        x="granularity",
        y="avg_runtime",
        color="ddl_command",
        labels={
            "target_object": "Target Object",
            "avg_runtime": "Avg. Runtime (s)",
            "granularity": "Granularity",
            "ddl_command": "DDL Command",
        },
        log_y=(y_axis_type == "Log"),  # Apply log scale if selected
    )

    fig.update_layout(legend_title="DDL Command", template="plotly_white", yaxis=dict(title="Avg. Runtime (s)"))
    st.plotly_chart(fig, use_container_width=True)


def plot_create(data_df, experiment_name, y_axis_type):
    # Create visualization for CREATE commands
    st.subheader(f"Average CREATE Query Runtime by Object & Granularity for {experiment_name.capitalize()}")
    with st.expander("Query Data"):
        st.dataframe(data_df, use_container_width=True)
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


def plot_ddl(data_df, ddl_command, experiment_name, y_axis_type):
    """
    Plot the average runtime for `ddl_command` commands
    """
    st.subheader(f"Average Runtime for {ddl_command} Commands in {experiment_name}")
    with st.expander("Query Data"):
        st.dataframe(data_df, use_container_width=True)

    fig = px.line(
        data_df,
        x="granularity",
        y="avg_runtime",
        color="target_object",
        labels={"target_object": "Target Object", "avg_runtime": "Avg. Runtime (s)", "granularity": "Granularity"},
        log_y=(y_axis_type == "Log"),  # Apply log scale if selected
    )

    st.plotly_chart(fig, use_container_width=True)


if __name__ == "__main__":
    category_map = {"Standard": "data/standard/", "Opendic": "data/opendic/", "Opendic(Batch)": "data/opendic_batch/"}
    create_dashboard(category_map[sidebar_category])
