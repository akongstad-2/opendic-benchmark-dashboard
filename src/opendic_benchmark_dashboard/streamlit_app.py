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
sidebar_category = st.sidebar.radio("Select Dataset Category", options=["TLDR", "Standard", "Opendic", "Opendic(Batch)"])


def create_dashboard(data_dir):
    data_files = [f for f in os.listdir(data_dir) if f.endswith(".parquet")]
    database_options = ["overview"] + sorted([os.path.splitext(f)[0] for f in data_files])

    # Sidebar for controls
    selected_db = st.sidebar.selectbox("Select Experiment", options=database_options, index=0)

    # Load data
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
        st.dataframe(data_df, use_container_width=True)

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
        summary_df,
        ddl_command="ALL",
        experiment_name="All standard datasystems",
        y_axis_type=y_axis_type,
        series_column="system_name",
        legend_title="System, DDL Command, Object Type",
        line_dash="ddl_command",
    )

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

    plot_summary(summary_df, ddl_command="SUMMARY", experiment_name=selected_db, y_axis_type=y_axis_type)
    plot_create(create_summary_df, experiment_name=selected_db, y_axis_type=y_axis_type)
    plot_ddl(alter_summary_df, "ALTER", experiment_name=selected_db, y_axis_type=y_axis_type)
    plot_ddl(comment_summary_df, "COMMENT", experiment_name=selected_db, y_axis_type=y_axis_type)
    plot_ddl(show_summary_df, "SHOW", experiment_name=selected_db, y_axis_type=y_axis_type)


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


def chunked_avg_runtime(data_df, chunk_size=20):
    # Create chunked averages (each row represents the average of 20 rows)
    # assign chunk IDs to each row
    create_summary = data_df.reset_index(drop=True)
    create_summary["chunk_id"] = create_summary.index // chunk_size

    # group by these chunk IDs and compute the average for each chunk
    return create_summary.groupby(["system_name", "ddl_command", "chunk_id", "target_object"], as_index=False).agg(
        avg_runtime=("avg_runtime", "mean"),
        granularity=("granularity", lambda x: x.iloc[0]),  # Take the first granularity value from each chunk
    )


def plot_summary(
    data_df,
    experiment_name,
    ddl_command,
    y_axis_type,
    series_column="ddl_command",
    legend_title="DDL Command",
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

    fig.update_layout(legend_title=legend_title, template="plotly_white", yaxis=dict(title="Avg. Runtime (s)"))
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
        labels={
            "target_object": "Target Object",
            "avg_runtime": "Avg. Runtime (s)",
            "granularity": "Granularity",
            "ddl_command": "DDL Command",
            "system_name": "System Name",
        },
        log_y=(y_axis_type == "Log"),  # Apply log scale if selected
    )

    st.plotly_chart(fig, use_container_width=True)


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
        yaxis=dict(title="Avg. Runtime (s)"),
    )
    st.plotly_chart(fig, use_container_width=True)


def create_tldr_dashboard(category_map: dict[str, str]):
    datafiles = []
    for path in category_map.values():
        path_data_files = [path + f for f in os.listdir(path) if f.endswith(".parquet")]
        datafiles.extend(path_data_files)

    data_df = pd.concat(
        [pd.read_parquet(data_file, engine="pyarrow") for data_file in datafiles],
        ignore_index=False,
    )

    y_axis_type = st.sidebar.selectbox("Y-axis scale", options=["Linear", "Log"], index=0)

    # Display raw data in expandable section
    with st.expander("View Raw Data (Partial of file size)"):
        st.dataframe(data_df.iloc[::10], use_container_width=True)

    plot_experiment_total_runtime(data_df=data_df)


def plot_experiment_total_runtime(data_df):
    """
    Plots the total runtime for each experiment/database as a horizontal bar chart.

    Args:
        data_df (pd.DataFrame): Dataframe containing the benchmark data.
        y_axis_type (str): Type of y-axis scale (Linear or Log).
    """
    st.subheader("Total Runtime by Experiment/Database")

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

    total_runtime_df["total_runtime"] = (total_runtime_df["total_runtime"] / 60 / 60 ).round(2)

    with st.expander("View Total Runtime Data"):
        st.dataframe(avg_runtime_df, use_container_width=True)

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
        margin=dict(l=20, r=20, t=30, b=20),
        xaxis=dict(title="Total Runtime (hours)"),
    )

    st.plotly_chart(fig, use_container_width=True)


if __name__ == "__main__":
    category_map = {"Standard": "data/standard/", "Opendic": "data/opendic/", "Opendic(Batch)": "data/opendic_batch/"}

    if sidebar_category != "TLDR":
        create_dashboard(category_map[sidebar_category])
    else:
        create_tldr_dashboard(category_map)
