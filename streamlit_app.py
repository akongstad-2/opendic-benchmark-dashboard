import os

import pandas as pd
import plotly.express as px
import streamlit as st

from opendic_benchmark_dashboard import storage_data

# Set page title and layout
st.set_page_config(page_title="OpenDIC Benchmark Dashboard", layout="wide")

# Add title and description
st.title("OpenDIC Benchmark Dashboard")
st.write("Visualize and compare benchmark results for different databases")

# Create tabs for switching between different dataset categories
st.sidebar.header("Dashboard Controls")
sidebar_category = st.sidebar.radio("Select Dataset Category", options=["TLDR", "Standard", "Opendic", "Opendic(Batch)"])


@st.cache_data(ttl="1h")
def load_data_standard(selected_db: str, data_dir: str, database_options):
    if selected_db != "overview":
        data_file = f"{data_dir}{selected_db}.parquet"
        data_df = pd.read_parquet(data_file, engine="pyarrow")

    else:
        data_df = pd.concat(
            [pd.read_parquet(f"{data_dir}{db}.parquet", engine="pyarrow") for db in database_options if db != "overview"],
            ignore_index=False,
        )

    # Display raw data in expandable section
    with st.expander("View Raw Data"):
        mem_bytes = data_df.memory_usage(deep=True).sum()
        mem_mb = mem_bytes / (1024**2)
        if mem_mb > 50:
            st.warning(f"Dataframe size is {mem_mb:.2f} MB. Showing Compacted")
            st.dataframe(data_df.iloc[::10], use_container_width=True)
        else:
            st.dataframe(data_df, use_container_width=True)

    return data_df


def create_dashboard(data_dir):
    data_files = [f for f in os.listdir(data_dir) if f.endswith(".parquet")]
    database_options = ["overview"] + sorted([os.path.splitext(f)[0] for f in data_files])

    # Sidebar for controls
    selected_db = st.sidebar.selectbox("Select Experiment", options=database_options, index=0)

    data_df = load_data_standard(selected_db, data_dir, database_options)

    if sidebar_category == "Standard":
        if selected_db == "overview":
            standard_compare_all_dashboard(data_df)
        else:
            standard_dashboard(data_df, selected_db=selected_db)
    elif sidebar_category == "Opendic":
        if selected_db == "overview":
            opendic_compare_all_dashboard(data_df)
        else:
            opendic_dashboard(data_df, selected_db=selected_db)
    elif sidebar_category == "Opendic(Batch)":
        if selected_db == "overview":
            opendic_batch_compare_all_dashboard(data_df)
        else:
            opendic_batch_dashboard(data_df, selected_db=selected_db)


def standard_dashboard(data_df, selected_db):
    # Overview dashboard
    # Filter for 'CREATE' commands and average runtimes
    create_df = (
        data_df[data_df["ddl_command"] == "CREATE"]
        .groupby(["system_name", "ddl_command", "target_object", "granularity"], as_index=False)
        .agg(avg_runtime=(("query_runtime"), "mean"))
    )
    create_summary_df = chunked_avg_runtime(
        create_df,
        chunk_size=20,
    )
    alter_summary_df = (
        data_df[data_df["ddl_command"] == "ALTER"]
        .groupby(["system_name", "ddl_command", "target_object", "granularity"], as_index=False)
        .agg(avg_runtime=(("query_runtime"), "mean"))
    )
    comment_summary_df = (
        data_df[data_df["ddl_command"] == "COMMENT"]
        .groupby(["system_name", "ddl_command", "target_object", "granularity"], as_index=False)
        .agg(avg_runtime=(("query_runtime"), "mean"))
    )
    show_summary_df = (
        data_df[data_df["ddl_command"] == "SHOW"]
        .groupby(["system_name", "ddl_command", "target_object", "granularity"], as_index=False)
        .agg(avg_runtime=(("query_runtime"), "mean"))
    )
    # Combine all summaries
    small_create_df = chunked_avg_runtime(create_df, chunk_size=250)
    summary_df = pd.concat([small_create_df, alter_summary_df, comment_summary_df, show_summary_df])

    # Add y-axis type control to sidebar
    y_axis_type = st.sidebar.selectbox("Y-axis scale", options=["Linear", "Log"], index=1)

    plot_create(create_summary_df, experiment_name=selected_db, y_axis_type=y_axis_type)
    plot_ddl(alter_summary_df, "ALTER", experiment_name=selected_db, y_axis_type=y_axis_type)
    plot_ddl(comment_summary_df, "COMMENT", experiment_name=selected_db, y_axis_type=y_axis_type)
    plot_ddl(show_summary_df, "SHOW", experiment_name=selected_db, y_axis_type=y_axis_type)
    plot_summary(
        summary_df,
        ddl_command="ALL",
        experiment_name=selected_db,
        y_axis_type=y_axis_type,
        series_column="ddl_command",
        line_dash="target_object",
        legend_orientation="v",
    )


def standard_compare_all_dashboard(data_df):
    create_df = (
        data_df[data_df["ddl_command"] == "CREATE"]
        .groupby(["system_name", "ddl_command", "target_object", "granularity"], as_index=False)
        .agg(avg_runtime=(("query_runtime"), "mean"))
    )
    create_summary_df = chunked_avg_runtime(
        create_df,
        chunk_size=20,
    )
    alter_summary_df = (
        data_df[data_df["ddl_command"] == "ALTER"]
        .groupby(["system_name", "ddl_command", "target_object", "granularity"], as_index=False)
        .agg(avg_runtime=(("query_runtime"), "mean"))
    )
    comment_summary_df = (
        data_df[data_df["ddl_command"] == "COMMENT"]
        .groupby(["system_name", "ddl_command", "target_object", "granularity"], as_index=False)
        .agg(avg_runtime=(("query_runtime"), "mean"))
    )
    show_summary_df = (
        data_df[data_df["ddl_command"] == "SHOW"]
        .groupby(["system_name", "ddl_command", "target_object", "granularity"], as_index=False)
        .agg(avg_runtime=(("query_runtime"), "mean"))
    )
    # Combine all summaries
    small_create_df = chunked_avg_runtime(create_df, chunk_size=1000)
    all_df = pd.concat([small_create_df, alter_summary_df, comment_summary_df, show_summary_df])

    # Create a summary_df that averages across target_object
    summary_df = all_df.groupby(["system_name", "ddl_command", "granularity"], as_index=False).agg(
        avg_runtime=("avg_runtime", "mean")
    )

    # Add y-axis type control to sidebar
    y_axis_type = st.sidebar.selectbox("Y-axis scale", options=["Linear", "Log"], index=1)

    plot_summary(
        create_summary_df,
        ddl_command="CREATE",
        experiment_name="All standard datasystems",
        y_axis_type=y_axis_type,
        series_column="system_name",
        legend_title="CREATE: System, Object Type",
        line_dash="target_object",
    )
    plot_summary(
        alter_summary_df,
        ddl_command="ALTER",
        experiment_name="All standard datasystems",
        y_axis_type=y_axis_type,
        series_column="system_name",
        legend_title="ALTER: System, Object Type",
        line_dash="target_object",
    )

    plot_summary(
        comment_summary_df,
        ddl_command="COMMENT",
        experiment_name="All standard datasystems",
        y_axis_type=y_axis_type,
        series_column="system_name",
        legend_title="COMMENT: System, Object Type",
        line_dash="target_object",
    )

    plot_summary(
        show_summary_df,
        ddl_command="SHOW",
        experiment_name="All standard datasystems",
        y_axis_type=y_axis_type,
        series_column="system_name",
        legend_title="SHOW: System, Object Type",
        line_dash="target_object",
    )
    plot_summary(
        summary_df,
        ddl_command="ALL",
        experiment_name="All standard datasystems",
        y_axis_type=y_axis_type,
        series_column="system_name",
        legend_title="System, Object Type",
        line_dash="ddl_command",
        legend_orientation="v",
    )


def opendic_dashboard(data_df, selected_db):
    # Filter for 'CREATE' commands and average runtimes
    create_df = (
        data_df[data_df["ddl_command"] == "CREATE"]
        .groupby(["system_name", "ddl_command", "target_object", "granularity"], as_index=False)
        .agg(avg_runtime=(("query_runtime"), "mean"))
    )
    create_summary_df = chunked_avg_runtime(
        create_df,
        chunk_size=20,
    )
    alter_summary_df = (
        data_df[data_df["ddl_command"] == "ALTER"]
        .groupby(["system_name", "ddl_command", "target_object", "granularity"], as_index=False)
        .agg(avg_runtime=(("query_runtime"), "mean"))
    )
    comment_summary_df = (
        data_df[data_df["ddl_command"] == "COMMENT"]
        .groupby(["system_name", "ddl_command", "target_object", "granularity"], as_index=False)
        .agg(avg_runtime=(("query_runtime"), "mean"))
    )
    show_summary_df = (
        data_df[data_df["ddl_command"] == "SHOW"]
        .groupby(["system_name", "ddl_command", "target_object", "granularity"], as_index=False)
        .agg(avg_runtime=(("query_runtime"), "mean"))
    )
    small_create_df = chunked_avg_runtime(create_df, chunk_size=250)
    summary_df = pd.concat([small_create_df, alter_summary_df, comment_summary_df, show_summary_df])

    # Add y-axis type control to sidebar
    y_axis_type = st.sidebar.selectbox("Y-axis scale", options=["Linear", "Log"], index=1)

    # Plot the summary dataframes
    plot_create(create_summary_df, experiment_name=selected_db, y_axis_type=y_axis_type)
    plot_ddl(alter_summary_df, "ALTER", experiment_name=selected_db, y_axis_type=y_axis_type)
    plot_ddl(comment_summary_df, "COMMENT", experiment_name=selected_db, y_axis_type=y_axis_type)
    plot_ddl(show_summary_df, "SHOW", experiment_name=selected_db, y_axis_type=y_axis_type)
    plot_summary(summary_df, ddl_command="ALL", experiment_name=selected_db, y_axis_type=y_axis_type)


def opendic_compare_all_dashboard(data_df):
    create_df = (
        data_df[data_df["ddl_command"] == "CREATE"]
        .groupby(["system_name", "ddl_command", "target_object", "granularity"], as_index=False)
        .agg(avg_runtime=(("query_runtime"), "mean"))
    )
    create_summary_df = chunked_avg_runtime(
        create_df,
        chunk_size=20,
    )
    alter_summary_df = (
        data_df[data_df["ddl_command"] == "ALTER"]
        .groupby(["system_name", "ddl_command", "target_object", "granularity"], as_index=False)
        .agg(avg_runtime=(("query_runtime"), "mean"))
    )
    comment_summary_df = (
        data_df[data_df["ddl_command"] == "COMMENT"]
        .groupby(["system_name", "ddl_command", "target_object", "granularity"], as_index=False)
        .agg(avg_runtime=(("query_runtime"), "mean"))
    )
    show_summary_df = (
        data_df[data_df["ddl_command"] == "SHOW"]
        .groupby(["system_name", "ddl_command", "target_object", "granularity"], as_index=False)
        .agg(avg_runtime=(("query_runtime"), "mean"))
    )
    # Combine all summaries
    small_create_df = chunked_avg_runtime(create_df, chunk_size=1000)
    all_df = pd.concat([small_create_df, alter_summary_df, comment_summary_df, show_summary_df])

    # Create a summary_df that averages across target_object
    summary_df = all_df.groupby(["system_name", "ddl_command", "granularity"], as_index=False).agg(
        avg_runtime=("avg_runtime", "mean")
    )

    # Add y-axis type control to sidebar
    y_axis_type = st.sidebar.selectbox("Y-axis scale", options=["Linear", "Log"], index=1)

    plot_summary(
        create_summary_df,
        ddl_command="CREATE",
        experiment_name="All standard datasystems",
        y_axis_type=y_axis_type,
        series_column="system_name",
        legend_title="CREATE: System, Object Type",
    )
    plot_summary(
        alter_summary_df,
        ddl_command="ALTER",
        experiment_name="All standard datasystems",
        y_axis_type=y_axis_type,
        series_column="system_name",
        legend_title="ALTER: System, Object Type",
    )

    plot_summary(
        comment_summary_df,
        ddl_command="COMMENT",
        experiment_name="All standard datasystems",
        y_axis_type=y_axis_type,
        series_column="system_name",
        legend_title="COMMENT: System, Object Type",
    )

    plot_summary(
        show_summary_df,
        ddl_command="SHOW",
        experiment_name="All standard datasystems",
        y_axis_type=y_axis_type,
        series_column="system_name",
        legend_title="SHOW: System, Object Type",
    )

    plot_summary(
        summary_df,
        ddl_command="ALL",
        experiment_name="All standard datasystems",
        y_axis_type=y_axis_type,
        series_column="system_name",
        legend_title="System, DDL Command, Object Type",
        line_dash="ddl_command",
    )


def opendic_batch_dashboard(data_df, selected_db: str):
    # Filter for 'CREATE' commands and average runtimes
    create_summary_df = (
        data_df[data_df["ddl_command"] == "CREATE"]
        .groupby(["system_name", "ddl_command", "target_object", "granularity"], as_index=False)
        .agg(avg_runtime=("query_runtime", "sum"))
    )
    alter_summary_df = (
        data_df[data_df["ddl_command"] == "ALTER"]
        .groupby(["system_name", "ddl_command", "target_object", "granularity"], as_index=False)
        .agg(avg_runtime=(("query_runtime"), "mean"))
    )
    comment_summary_df = (
        data_df[data_df["ddl_command"] == "COMMENT"]
        .groupby(["system_name", "ddl_command", "target_object", "granularity"], as_index=False)
        .agg(avg_runtime=(("query_runtime"), "mean"))
    )
    show_summary_df = (
        data_df[data_df["ddl_command"] == "SHOW"]
        .groupby(["system_name", "ddl_command", "target_object", "granularity"], as_index=False)
        .agg(avg_runtime=(("query_runtime"), "mean"))
    )
    summary_df = pd.concat([create_summary_df, alter_summary_df, comment_summary_df, show_summary_df])

    # Add y-axis type control to sidebar
    y_axis_type = st.sidebar.selectbox("Y-axis scale", options=["Linear", "Log"], index=1)

    plot_create(create_summary_df, experiment_name=selected_db, y_axis_type=y_axis_type)
    plot_ddl(alter_summary_df, "ALTER", experiment_name=selected_db, y_axis_type=y_axis_type)
    plot_ddl(comment_summary_df, "COMMENT", experiment_name=selected_db, y_axis_type=y_axis_type)
    plot_ddl(show_summary_df, "SHOW", experiment_name=selected_db, y_axis_type=y_axis_type)
    plot_summary(summary_df, ddl_command="SUMMARY", experiment_name=selected_db, y_axis_type=y_axis_type)


def opendic_batch_compare_all_dashboard(data_df):
    create_summary_df = (
        data_df[data_df["ddl_command"] == "CREATE"]
        .groupby(["granularity", "system_name", "ddl_command", "target_object"], as_index=False)
        .agg(avg_runtime=("query_runtime", "sum"))
    )
    alter_summary_df = (
        data_df[data_df["ddl_command"] == "ALTER"]
        .groupby(["granularity", "system_name", "ddl_command", "target_object"], as_index=False)
        .agg(avg_runtime=(("query_runtime"), "mean"))
    )
    comment_summary_df = (
        data_df[data_df["ddl_command"] == "COMMENT"]
        .groupby(["granularity", "system_name", "ddl_command", "target_object"], as_index=False)
        .agg(avg_runtime=(("query_runtime"), "mean"))
    )
    show_summary_df = (
        data_df[data_df["ddl_command"] == "SHOW"]
        .groupby(["granularity", "system_name", "ddl_command", "target_object"], as_index=False)
        .agg(avg_runtime=(("query_runtime"), "mean"))
    )
    # Combine all summaries
    all_df = pd.concat([create_summary_df, alter_summary_df, comment_summary_df, show_summary_df])

    # Create a summary_df that averages across target_object
    summary_df = all_df.groupby(["granularity", "system_name", "ddl_command"], as_index=False).agg(
        avg_runtime=("avg_runtime", "mean")
    )

    # Add y-axis type control to sidebar
    y_axis_type = st.sidebar.selectbox("Y-axis scale", options=["Linear", "Log"], index=1)

    plot_histo(
        summary_df,
        ddl_command="ALL",
        experiment_name="All standard datasystems",
        y_axis_type=y_axis_type,
        series_column="system_name",
        additional_column="ddl_command",
        legend_title="System | DDL Command",
    )

    plot_histo(
        create_summary_df,
        ddl_command="CREATE",
        experiment_name="BATCHED CREATE with OPENDIC",
        y_axis_type=y_axis_type,
        series_column="system_name",
        legend_title="CREATE: System, Object Type",
    )
    plot_histo(
        alter_summary_df,
        ddl_command="ALTER",
        experiment_name="BATCHED CREATE with OPENDIC",
        y_axis_type=y_axis_type,
        series_column="system_name",
        legend_title="ALTER: System, Object Type",
    )

    plot_histo(
        comment_summary_df,
        ddl_command="COMMENT",
        experiment_name="BATCHED CREATE with OPENDIC",
        y_axis_type=y_axis_type,
        series_column="system_name",
        legend_title="COMMENT: System, Object Type",
    )

    plot_histo(
        show_summary_df,
        ddl_command="SHOW",
        experiment_name="BATCHED CREATE with OPENDIC",
        y_axis_type=y_axis_type,
        series_column="system_name",
        legend_title="SHOW: System, Object Type",
    )


@st.cache_data
def chunked_avg_runtime(data_df, chunk_size=20, columns=["system_name", "ddl_command", "target_object"]):
    """
    Args:
        columns: List of columns to group by for computing chunked averages.
    """
    # Create chunked averages (each row represents the average of 20 rows)
    # assign chunk IDs to each row
    create_summary = data_df.reset_index(drop=True)
    create_summary["chunk_id"] = create_summary.index // chunk_size

    # group by these chunk IDs and compute the average for each chunk
    return create_summary.groupby(columns + ["chunk_id"], as_index=False).agg(
        avg_runtime=("avg_runtime", "mean"),
        granularity=("granularity", lambda x: x.iloc[0]),  # Take the first granularity value from each chunk
    )


@st.cache_data
def plot_summary(
    data_df,
    experiment_name,
    ddl_command,
    y_axis_type,
    series_column="ddl_command",
    legend_title="DDL Command",
    legend_orientation="h",
    line_dash=None,
    markers: bool = False,
    symbol=None,
):
    """
    Args:
        data_df (pd.DataFrame): Dataframe containing the data to be plotted.
        experiment_name (str): Name of the experiment. (selected_db)
        ddl_command (str): Type of DDL command.
        y_axis_type (str): Type of y-axis scale. (Log, Linear)
        series_column (str): Column name for the series.
        legend_title (str): Title for the legend. (Legend_title for series)
        line_dash (str): Line style for the plot.
        markers (bool): Whether to show markers on the plot.
    """
    st.subheader(f"Average Runtime for {ddl_command} Commands in {experiment_name}")
    with st.expander("Query Data"):
        st.dataframe(data_df, use_container_width=True)

    fig = px.line(
        data_df,
        x="granularity",
        y="avg_runtime",
        color=series_column,
        line_dash=line_dash,
        markers=markers,
        symbol=symbol,
        labels={
            "target_object": "Target Object",
            "avg_runtime": "Avg. Runtime (s)",
            "granularity": "Granularity",
            "ddl_command": "DDL Command",
            "system_name": "System Name",
        },
        log_y=(y_axis_type == "Log"),  # Apply log scale to y-axis if selected
    )

    fig.update_layout(
        legend_title=legend_title,
        template="plotly_white",
        yaxis=dict(title="Avg. Runtime (s)", exponentformat="none"),
        legend=dict(
            orientation=legend_orientation,
            xanchor="center",  # anchor at center
            yanchor="bottom",  # anchor on bottom of text
            x=0.5,  # horizontal center
            y=1.0,  # just above the plotting area
        )
        if legend_orientation == "h"
        else None,
    )
    # Add a config to enable SVG export via the modebar
    config = {
        "toImageButtonOptions": {
            "format": "svg",  # Default to svg format
            "filename": "total_runtime_chart",
            # "height": 500,
            # "width": 1000,
            "scale": 1,
        },
        "displaylogo": False,
        "modeBarButtonsToAdd": ["downloadSVG"],
    }

    # Display the chart with export configuration
    st.plotly_chart(fig, use_container_width=True, config=config)


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
        labels={
            "target_object": "Target Object",
            "avg_runtime": "Avg. Runtime (s)",
            "granularity": "Granularity",
            "ddl_command": "DDL Command",
            "system_name": "System Name",
        },
        log_y=(y_axis_type == "Log"),  # Apply log scale if selected
    )

    fig.update_layout(
        xaxis_tickangle=-45,
        legend_title="Object Type",
        template="plotly_white",
        yaxis=dict(title="Avg. Runtime (s)", exponentformat="none")
        if y_axis_type == "Log"
        else dict(
            title="Avg. Runtime (s)",
            range=[0, data_df["avg_runtime"].quantile(0.999)],  # Remove blantant outliers
        ),
        legend=dict(
            orientation="h",
            xanchor="center",  # anchor at center
            yanchor="bottom",  # anchor on bottom of text
            x=0.5,  # horizontal center
            y=1.0,  # just above the plotting area
        ),
    )
    # Add a config to enable SVG export via the modebar
    config = {
        "toImageButtonOptions": {
            "format": "svg",  # Default to svg format
            "filename": "total_runtime_chart",
            # "height": 500,
            # "width": 1000,
            "scale": 1,
        },
        "displaylogo": False,
        "modeBarButtonsToAdd": ["downloadSVG"],
    }

    # Display the chart with export configuration
    st.plotly_chart(fig, use_container_width=True, config=config)


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
        labels={
            "target_object": "Target Object",
            "avg_runtime": "Avg. Runtime (s)",
            "granularity": "Granularity",
            "ddl_command": "DDL Command",
            "system_name": "System Name",
        },
        log_y=(y_axis_type == "Log"),  # Apply log scale if selectedm
    )

    fig.update_layout(
        legend=dict(
            orientation="h",
            xanchor="center",  # anchor at center
            yanchor="bottom",  # anchor on bottom of text
            x=0.5,  # horizontal center
            y=1.0,  # just above the plotting area
        ),
    )

    # Add a config to enable SVG export via the modebar
    config = {
        "toImageButtonOptions": {
            "format": "svg",  # Default to svg format
            "filename": "total_runtime_chart",
            # "height": 500,
            # "width": 1000,
            "scale": 1,
        },
        "displaylogo": False,
        "modeBarButtonsToAdd": ["downloadSVG"],
    }

    # Display the chart with export configuration
    st.plotly_chart(fig, use_container_width=True, config=config)


def plot_histo(
    data_df,
    experiment_name,
    ddl_command,
    y_axis_type,
    series_column="ddl_command",
    additional_column=None,  # New argument for an additional column
    legend_title="DDL Command",
    marginal=None,
    bar_mode="group",
):
    """
    Args:
        data_df (pd.DataFrame): Dataframe containing the data to be plotted.
        experiment_name (str): Name of the experiment. (selected_db)
        ddl_command (str): Type of DDL command.
        y_axis_type (str): Type of y-axis scale. (Log, Linear)
        series_column (str): Column name for the series.
        legend_title (str): Title for the legend. (Legend_title for series)
        line_dash (str): Line style for the plot.
        markers (bool): Whether to show markers on the plot.
    """
    st.subheader(f"Average Runtime for {ddl_command} Commands in {experiment_name}")
    with st.expander("Query Data"):
        st.dataframe(data_df, use_container_width=True)

    # Combine series_column and additional_column if provided
    if additional_column:
        data_df["combined_series"] = data_df[series_column] + " | " + data_df[additional_column]
        color_column = "combined_series"
    else:
        color_column = series_column
    data_df["granularity"] = data_df["granularity"].astype(str)  # Make sure x-axis is string not int

    fig = px.histogram(
        data_df,
        x="granularity",
        y="avg_runtime",
        color=color_column,
        labels={
            "target_object": "Target Object",
            "avg_runtime": "Avg. Runtime (s)",
            "granularity": "Granularity",
            "ddl_command": "DDL Command",
            "system_name": "System Name",
        },
        marginal=marginal,
        log_y=(y_axis_type == "Log"),  # Apply log scale to y-axis if selected
    )

    fig.update_layout(
        legend_title=legend_title,
        template="plotly_white",
        barmode=bar_mode,
        yaxis=dict(title="Avg. Runtime (s)", exponentformat="none"),
        legend=dict(
            orientation="h",
            x=0.5,  # horizontal center
            xanchor="center",
            y=1.0,  # just above the plotting area
            yanchor="bottom",
            font=dict(
                size=9  # Adjust this value to your preference (smaller number = smaller text)
            ),
        ),
    )

    # Add a config to enable SVG export via the modebar
    config = {
        "toImageButtonOptions": {
            "format": "svg",  # Default to svg format
            "filename": "total_runtime_chart",
            # "height": 500,
            # "width": 1000,
            "scale": 1,
        },
        "displaylogo": False,
        "modeBarButtonsToAdd": ["downloadSVG"],
    }

    # Display the chart with export configuration
    st.plotly_chart(fig, use_container_width=True, config=config)


@st.cache_data(ttl="1h")
def load_data():
    datafiles = []
    for path in category_map.values():
        path_data_files = [path + f for f in os.listdir(path) if f.endswith(".parquet")]
        datafiles.extend(path_data_files)

    data_df = pd.concat(
        [pd.read_parquet(data_file, engine="pyarrow") for data_file in datafiles],
        ignore_index=False,
    )
    with st.expander("View Raw Data (Partial of file size)"):
        st.dataframe(data_df.iloc[::10], use_container_width=True)
    return data_df


def create_tldr_dashboard(category_map: dict[str, str]):
    data_df = load_data()

    y_axis_type = st.sidebar.selectbox("Y-axis scale", options=["Linear", "Log"], index=1)

    plot_001_histo_experiment_total_runtime(data_df=data_df)
    plot_002_all_create_dashboard(data_df=data_df, y_axis_type=y_axis_type)
    plot_004_storage(data_df=storage_data.df_storage, y_axis_type=y_axis_type)
    plot_003_all_alter_commet_show(data_df=data_df, y_axis_type=y_axis_type)


def plot_005_opendic_optimization_overview(data_df, y_axis_type):
    # Remove polaris from system names
    data_df["system_name"] = data_df["system_name"].str.replace("_polaris", "", regex=True)

    with st.expander("Show Raw Data"):
        st.dataframe(data_df)


@st.cache_data
def plot_004_storage(data_df, y_axis_type: str):
    # Display the raw data
    with st.expander("Show Raw Data"):
        st.dataframe(data_df)

    fig_storage = px.bar(
        data_df,
        x="Database System",
        y="Storage Usage (GB)",
        color="Database System",
        title="Storage Usage by Datasystem System (GB)",
        log_y=(y_axis_type == "Log"),
        labels={
            "Database System": "Data System",
            "Storage Usage (GB)": "Storage Usage (GB)",
            "Metadatafiles Count": "Metadatafiles",
            "Datafiles Count": "Datafiles",
        },
        hover_data={
            "Database System": True,
            "Storage Usage (GB)": True,
            "Metadatafiles Count": True,
            "Datafiles Count": True,
        },
    )
    fig_storage.update_layout(
        xaxis_title="System",
        yaxis_title="Storage Usage (GB)",
        legend=dict(
            orientation="h",
            x=0.5,  # horizontal center
            xanchor="center",
            y=1.0,  # just above the plotting area
            yanchor="bottom",
        ),
    )
    # Add a config to enable SVG export via the modebar
    config = {
        "toImageButtonOptions": {
            "format": "svg",  # Default to svg format
            "filename": "total_runtime_chart",
            # "height": 500,
            # "width": 1000,
            "scale": 1,
        },
        "displaylogo": False,
        "modeBarButtonsToAdd": ["downloadSVG"],
    }

    # Display the chart with export configuration
    st.plotly_chart(fig_storage, use_container_width=True, config=config)


@st.cache_data
def plot_003_all_alter_commet_show(data_df, y_axis_type: str):
    processed_df = data_df.copy()
    # Remove "_batch" and "_cache" from system_names
    processed_df["system_name"] = processed_df["system_name"].str.replace("_batch|_cache", "", regex=True)

    alter_summary_df = (
        processed_df[processed_df["ddl_command"] == "ALTER"]
        .groupby(["system_name", "ddl_command", "granularity"], as_index=False)
        .agg(avg_runtime=(("query_runtime"), "mean"))
    )
    comment_summary_df = (
        processed_df[processed_df["ddl_command"] == "COMMENT"]
        .groupby(["system_name", "ddl_command", "granularity"], as_index=False)
        .agg(avg_runtime=(("query_runtime"), "mean"))
    )
    show_summary_df = (
        processed_df[processed_df["ddl_command"] == "SHOW"]
        .groupby(["system_name", "ddl_command", "granularity"], as_index=False)
        .agg(avg_runtime=(("query_runtime"), "mean"))
    )

    plot_summary(
        alter_summary_df,
        ddl_command="ALTER",
        experiment_name="ALL",
        y_axis_type=y_axis_type,
        series_column="system_name",
        legend_title="System Name",
    )

    plot_summary(
        comment_summary_df,
        ddl_command="COMMENT",
        experiment_name="ALL",
        y_axis_type=y_axis_type,
        series_column="system_name",
        legend_title="System Name",
    )

    plot_summary(
        show_summary_df,
        ddl_command="SHOW",
        experiment_name="ALL",
        y_axis_type=y_axis_type,
        series_column="system_name",
        legend_title="System Name",
    )


@st.cache_data(ttl="1h")
def plot_002_all_create_dashboard(data_df, y_axis_type: str):
    create_df = (
        data_df[
            (data_df["ddl_command"] == "CREATE")
            & (~data_df["system_name"].str.contains("batch", case=False, na=False))
            & (~data_df["system_name"].str.contains("cache", case=False, na=False))
        ]
        .groupby(["system_name", "ddl_command", "granularity"], as_index=False)
        .agg(avg_runtime=("query_runtime", "mean"))
    )
    create_summary_df = chunked_avg_runtime(create_df, chunk_size=50, columns=["system_name", "ddl_command"])

    plot_summary(
        create_summary_df,
        ddl_command="CREATE",
        experiment_name="ALL",
        y_axis_type=y_axis_type,
        series_column="system_name",
        legend_title="System Name",
    )


@st.cache_data(ttl="1h")
def plot_001_histo_experiment_total_runtime(data_df):
    """
    Plots the total runtime for each experiment/database as a horizontal bar chart.

    Args:
        data_df (pd.DataFrame): Dataframe containing the benchmark data.
        y_axis_type (str): Type of y-axis scale (Linear or Log).
    """
    st.subheader("Total Runtime by Experiment/Database")

    # Remove polaris from system names
    data_df["system_name"] = data_df["system_name"].str.replace("_polaris", "", regex=True)

    # Calculate average runtime for each unique operation to account for repetitions
    avg_runtime_df = data_df.groupby(["system_name", "ddl_command", "granularity"], as_index=False).agg(
        avg_runtime=("query_runtime", "mean")
    )

    # Sum the average runtimes for each system to get total runtime
    total_runtime_df = (
        avg_runtime_df.groupby("system_name", as_index=False)
        .agg(total_runtime=("avg_runtime", "sum"))
        .sort_values("total_runtime", ascending=True)
    )  # Sort for better visualization

    total_runtime_df["total_runtime"] = (total_runtime_df["total_runtime"] / 60 / 60).round(4)

    with st.expander("View Total Runtime Data"):
        st.dataframe(total_runtime_df, use_container_width=True)

    # Create horizontal bar chart
    fig = px.bar(
        total_runtime_df,
        y="system_name",
        x="total_runtime",
        orientation="h",
        labels={"system_name": "Database/Experiment", "total_runtime": "Total Runtime (hours)"},
        color="system_name",  # Color bars by system name
    )

    fig.update_layout(
        template="plotly_white",
        showlegend=False,  # No need for legend as y-axis shows the system names
        xaxis=dict(title="Total Runtime (hours)"),
    )

    # Add SVG export capability
    # Add a config to enable SVG export via the modebar
    config = {
        "toImageButtonOptions": {
            "format": "svg",  # Default to svg format
            "filename": "total_runtime_chart",
            # "height": 500,
            # "width": 1000,
            "scale": 1,
        },
        "displaylogo": False,
        "modeBarButtonsToAdd": ["downloadSVG"],
    }

    # Display the chart with export configuration
    st.plotly_chart(fig, use_container_width=True, config=config)


if __name__ == "__main__":
    category_map = {"Standard": "data/standard/", "Opendic": "data/opendic/", "Opendic(Batch)": "data/opendic_batch/"}

    if sidebar_category != "TLDR":
        create_dashboard(category_map[sidebar_category])
    else:
        create_tldr_dashboard(category_map)
