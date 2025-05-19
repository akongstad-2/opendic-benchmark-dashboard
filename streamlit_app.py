import os

import duckdb
import pandas as pd
import plotly.colors as pc
import plotly.express as px
import streamlit as st
from pandas.core.common import np

from opendic_benchmark_dashboard import storage_data

LEGEND_FONT_SIZE = 18
ALL_DATA_VIEW = "all_data"
SYSTEM_ORDER = [
    "sqlite",
    "duckDB",
    "snowflake",
    "postgres",
    "opendict_polaris_file",
    "opendict_polaris_file_cache",
    "opendict_polaris_file_batch",
    "opendict_polaris_file_cache_batch",
    "opendict_polaris_cloud_azure_cached",
    "opendict_polaris_cloud_azure_cached_batch",
]
SYSTEM_LABEL_ORDER = [
    "sqlite",
    "duckdb",
    "snowflake",
    "postgres",
    "opendict (local)",
    "opendict (local, cache)",
    "opendict (local, batch)",
    "opendict (local, cache, batch)",
    "opendict (cloud, cache)",
    "opendict (cloud, cache, batch)",
]

OPENDICT_LABELS = {
    "sqlite": "sqlite",
    "duckDB": "duckdb",
    "snowflake": "snowflake",
    "postgres": "postgres",
    "opendict_polaris_file": "opendict (local)",
    "opendict_polaris_file_batch": "opendict (local, batch)",
    "opendict_polaris_file_cache": "opendict (local, cache)",
    "opendict_polaris_file_cache_batch": "opendict (local, cache, batch)",
    "opendict_polaris_cloud_azure_cached": "opendict (cloud, cache)",
    "opendict_polaris_cloud_azure_cached_batch": "opendict (cloud, cache, batch)",
}

# Reverse mapping to get full label list
SYSTEM_LABEL_ORDER = list(OPENDICT_LABELS.values())
n_opendict_variants = 6
narrow_range = np.linspace(0.8, 0.3, n_opendict_variants)
# Base color shades (e.g., blue)
opendict_color_shades = pc.sample_colorscale("blues", narrow_range, colortype="rgb")

# Build color map
SYSTEM_LABEL_COLOR_MAP = {}

# Assign default Plotly colors to non-opendict systems
non_opendict_labels = [label for label in SYSTEM_LABEL_ORDER if not label.startswith("opendict")]
st_colors = pc.qualitative.Plotly
for i, label in enumerate(non_opendict_labels):
    SYSTEM_LABEL_COLOR_MAP[label] = st_colors[i % len(st_colors)]

# Assign shades of blue to opendict systems
opendict_labels = [label for label in SYSTEM_LABEL_ORDER if label.startswith("opendict")]
for i, label in enumerate(opendict_labels):
    SYSTEM_LABEL_COLOR_MAP[label] = opendict_color_shades[i % len(opendict_color_shades)]

# Add "opendict (cloud)" with same color as "opendict (cloud, cache)"
SYSTEM_LABEL_COLOR_MAP["opendict (cloud)"] = SYSTEM_LABEL_COLOR_MAP["opendict (cloud, cache)"]


# Set page title and layout
st.set_page_config(page_title="OpenDIC Benchmark Dashboard", layout="wide")

# Add title and description
st.title("OpenDIC Benchmark Dashboard")
st.write("Visualize and compare benchmark results for different databases")

# Create tabs for switching between different dataset categories
st.sidebar.header("Dashboard Controls")
sidebar_category = st.sidebar.radio("Select Dataset Category", options=["TLDR", "Standard", "Opendic", "Opendic_batch"])


def load_all_data():
    # Initialize DuckDB connection
    conn = duckdb.connect(database=":memory:")

    # Create a list of all parquet files
    datafiles = []
    for path in category_map.values():
        path_data_files = [path + f for f in os.listdir(path) if f.endswith(".parquet")]
        datafiles.extend(path_data_files)

    # Register all parquet files as a view in DuckDB
    conn.execute(f"CREATE VIEW {ALL_DATA_VIEW} AS SELECT * FROM parquet_scan({datafiles})")

    return conn


def get_all_create_data(conn):
    """Get aggregated data for CREATE operations using DuckDB"""
    query = f"""
    SELECT
        system_name,
        ddl_command,
        granularity,
        AVG(query_runtime) as avg_runtime
    FROM {ALL_DATA_VIEW}
    WHERE ddl_command = 'CREATE'
    GROUP BY system_name, ddl_command, granularity
    ORDER BY granularity asc;
    """

    # Execute query and return as pandas DataFrame
    result = conn.execute(query).fetch_df()
    return result


def get_all_alter_data(conn):
    """Get aggregated data for ALTER operations using DuckDB"""
    query = f"""
    SELECT
        system_name,
        ddl_command,
        granularity,
        AVG(query_runtime) as avg_runtime
    FROM {ALL_DATA_VIEW}
    WHERE ddl_command = 'ALTER'
        AND LOWER(system_name) NOT LIKE '%batch%'
        AND LOWER(system_name) NOT LIKE 'opendict_polaris_file'
        -- AND LOWER(system_name) NOT LIKE 'opendict_polaris_cloud%'
    GROUP BY system_name, ddl_command, granularity
    ORDER BY granularity asc;
    """

    # Execute query and return as pandas DataFrame
    result = conn.execute(query).fetch_df()
    return result


def get_all_comment_data(conn):
    """Get aggregated data for COMMENT operations using DuckDB"""
    query = f"""
    SELECT
        system_name,
        ddl_command,
        granularity,
        AVG(query_runtime) as avg_runtime
    FROM {ALL_DATA_VIEW}
    WHERE ddl_command = 'COMMENT'
        AND LOWER(system_name) NOT LIKE '%batch%'
        AND LOWER(system_name) NOT LIKE 'opendict_polaris_file'
        -- AND LOWER(system_name) NOT LIKE 'opendict_polaris_cloud%'
    GROUP BY system_name, ddl_command, granularity
    ORDER BY granularity asc;
    """

    # Execute query and return as pandas DataFrame
    result = conn.execute(query).fetch_df()
    return result


def get_all_show_data(conn):
    """Get aggregated data for SHOW operations using DuckDB"""
    query = f"""
    SELECT
        system_name,
        ddl_command,
        granularity,
        AVG(query_runtime) as avg_runtime
    FROM {ALL_DATA_VIEW}
    WHERE ddl_command = 'SHOW'
    AND LOWER(system_name) NOT LIKE '%batch%'
    AND LOWER(system_name) NOT LIKE 'opendict_polaris_file'
    -- AND LOWER(system_name) NOT LIKE 'opendict_polaris_cloud%'
    GROUP BY system_name, ddl_command, granularity
    ORDER BY granularity asc;
    """

    # Execute query and return as pandas DataFrame
    result = conn.execute(query).fetch_df()
    return result


def get_data(conn, command, experiment: str):
    query: str = f"""
        SELECT
            system_name,
            ddl_command,
            target_object,
            granularity,
            AVG(query_runtime) AS avg_runtime
        FROM data_{experiment}
        WHERE ddl_command = '{command}'
        GROUP BY system_name, ddl_command, target_object, granularity
        ORDER BY granularity asc;
    """
    result = conn.execute(query).fetch_df()
    return result


def get_create_data_chunked(conn: duckdb.DuckDBPyConnection, experiment, chunk_size=50):
    query = f"""
    WITH avg_by_granularity AS (
        -- First average runtimes for entries with the same granularity
        SELECT
            system_name,
            ddl_command,
            target_object,
            granularity,
            AVG(query_runtime) AS avg_runtime
        FROM data_{experiment}
        WHERE ddl_command = 'CREATE'
        GROUP BY system_name, ddl_command, target_object, granularity
    ),
    chunked_data AS (
        -- Then apply chunking to the averaged data
        SELECT
            system_name,
            ddl_command,
            target_object,
            granularity,
            avg_runtime,
            CAST((ROW_NUMBER() OVER (ORDER BY system_name, ddl_command, target_object, granularity) - 1) / {chunk_size} AS INTEGER) AS chunk_id
        FROM avg_by_granularity
    )
    -- Finally, average within chunks
    SELECT
        system_name,
        ddl_command,
        target_object,
        chunk_id,
        AVG(avg_runtime) AS avg_runtime,
        FIRST(granularity) AS granularity
    FROM chunked_data
    GROUP BY system_name, ddl_command, target_object, chunk_id
    ORDER BY chunk_id ASC, granularity ASC;
    """
    # s ystem_name, ddl_command, target_object, granularity
    # GROUP BY system_name, ddl_command, target_object, chunk_id

    return conn.execute(query).fetch_df()


def plot_dataframe(data_df, text: str):
    with st.expander(text):
        mem_bytes = data_df.memory_usage(deep=True).sum()
        mem_mb = mem_bytes / (1024**2)
        if mem_mb > 5:
            st.warning(f"Dataframe size is {mem_mb:.2f} MB. Showing Compacted")
            while mem_mb > 5:
                data_df = data_df.iloc[::2]
                mem_bytes = data_df.memory_usage(deep=True).sum()
                mem_mb = mem_bytes / (1024**2)

        st.dataframe(data_df, use_container_width=True)


def create_dashboard(data_dir, conn):
    data_files = [f for f in os.listdir(data_dir) if f.endswith(".parquet")]
    database_options = ["overview"] + sorted([os.path.splitext(f)[0] for f in data_files])

    # Sidebar for controls
    selected_db = st.sidebar.selectbox("Select Experiment", options=database_options, index=0)
    data_files = [data_dir + f for f in data_files]

    if sidebar_category == "Standard":
        if selected_db == "overview":
            standard_compare_all_dashboard(conn, parquet_files=data_files, sidebar_category=sidebar_category)
        else:
            standard_dashboard(
                conn,
                parquet_files=[data_dir + selected_db + ".parquet"],
                sidebar_category=sidebar_category,
                selected_db=selected_db,
            )
    elif sidebar_category == "Opendic":
        if selected_db == "overview":
            standard_compare_all_dashboard(conn, parquet_files=data_files, sidebar_category=sidebar_category)
        else:
            standard_dashboard(
                conn,
                parquet_files=[data_dir + selected_db + ".parquet"],
                sidebar_category=sidebar_category,
                selected_db=selected_db,
            )
    elif sidebar_category == "Opendic_batch":
        if selected_db == "overview":
            opendic_batch(conn, parquet_files=data_files, sidebar_category=sidebar_category, experiment_name="ALL")
        else:
            opendic_batch(
                conn,
                parquet_files=[data_dir + selected_db + ".parquet"],
                sidebar_category=sidebar_category,
                experiment_name=selected_db,
            )


def standard_dashboard(conn, parquet_files: list[str], sidebar_category, selected_db):
    conn.execute(f"CREATE VIEW if not exists data_{sidebar_category} AS SELECT * FROM parquet_scan({parquet_files})")

    create_summary_df = get_create_data_chunked(conn, chunk_size=50, experiment=sidebar_category)
    alter_summary_df = get_data(conn, "ALTER", sidebar_category)
    comment_summary_df = get_data(conn, "COMMENT", sidebar_category)
    show_summary_df = get_data(conn, "SHOW", sidebar_category)
    # Combine all summaries
    small_create_df = get_create_data_chunked(conn, chunk_size=1000, experiment=sidebar_category)
    summary_df = pd.concat([small_create_df, alter_summary_df, comment_summary_df, show_summary_df])

    # Add y-axis type control to sidebar
    y_axis_type = st.sidebar.selectbox("Y-axis scale", options=["Linear", "Log"], index=1)

    plot_create(create_summary_df, experiment_name=selected_db, y_axis_type=y_axis_type)
    plot_ddl(alter_summary_df, ddl_command="ALTER", experiment_name=selected_db, y_axis_type=y_axis_type)
    plot_ddl(comment_summary_df, ddl_command="COMMENT", experiment_name=selected_db, y_axis_type=y_axis_type)
    plot_ddl(show_summary_df, ddl_command="SHOW", experiment_name=selected_db, y_axis_type=y_axis_type)
    plot_summary(
        summary_df,
        ddl_command="ALL",
        experiment_name=selected_db,
        y_axis_type=y_axis_type,
        series_column="ddl_command",
        line_dash="target_object",
    )


def standard_compare_all_dashboard(conn, parquet_files: list[str], sidebar_category):
    conn.execute(f"CREATE VIEW if not exists data_{sidebar_category} AS SELECT * FROM parquet_scan({parquet_files})")
    create_summary_df = get_create_data_chunked(conn, sidebar_category, chunk_size=50)
    alter_summary_df = get_data(conn, "ALTER", sidebar_category)
    comment_summary_df = get_data(conn, "COMMENT", sidebar_category)
    show_summary_df = get_data(conn, "SHOW", sidebar_category)

    # Combine all summaries
    small_create_df = get_create_data_chunked(conn, sidebar_category, chunk_size=1000)
    summary_df = pd.concat([small_create_df, alter_summary_df, comment_summary_df, show_summary_df])

    if sidebar_category == "Standard":
        # Create a summary_df that averages across target_object
        summary_df = summary_df.groupby(["system_name", "ddl_command", "granularity"], as_index=False).agg(
            avg_runtime=("avg_runtime", "mean")
        )
    # Add y-axis type control to sidebar
    y_axis_type = st.sidebar.selectbox("Y-axis scale", options=["Linear", "Log"], index=1)

    plot_summary(
        create_summary_df,
        ddl_command="CREATE",
        experiment_name="All standard datasystems",
        y_axis_type=y_axis_type,
        series_column="system_label",
        legend_title="CREATE: System, Object Type",
        line_dash="target_object",
    )
    plot_summary(
        alter_summary_df,
        ddl_command="ALTER",
        experiment_name="All standard datasystems",
        y_axis_type=y_axis_type,
        series_column="system_label",
        legend_title="ALTER: System, Object Type",
        line_dash="target_object",
        log_x=True,
        symbol="system_name",
    )

    plot_summary(
        comment_summary_df,
        ddl_command="COMMENT",
        experiment_name="All standard datasystems",
        y_axis_type=y_axis_type,
        series_column="system_label",
        legend_title="COMMENT: System, Object Type",
        line_dash="target_object",
        log_x=True,
        symbol="system_name",
    )

    plot_summary(
        show_summary_df,
        ddl_command="SHOW",
        experiment_name="All standard datasystems",
        y_axis_type=y_axis_type,
        series_column="system_label",
        legend_title="SHOW: System, Object Type",
        line_dash="target_object",
        log_x=True,
        symbol="system_name",
    )
    plot_summary(
        summary_df,
        ddl_command="ALL",
        experiment_name="All standard datasystems",
        y_axis_type=y_axis_type,
        series_column="system_label",
        legend_title="System, Object Type",
        line_dash="ddl_command",
    )


def opendic_batch(conn, parquet_files: list[str], sidebar_category, experiment_name):
    conn.execute(f"CREATE VIEW if not exists data_{sidebar_category} AS SELECT * FROM parquet_scan({parquet_files})")
    create_summary_df = get_data(conn, "CREATE", sidebar_category)
    alter_summary_df = get_data(conn, "ALTER", sidebar_category)
    comment_summary_df = get_data(conn, "COMMENT", sidebar_category)
    show_summary_df = get_data(conn, "SHOW", sidebar_category)
    summary_df = pd.concat([create_summary_df, alter_summary_df, comment_summary_df, show_summary_df])

    # Add y-axis type control to sidebar
    y_axis_type = st.sidebar.selectbox("Y-axis scale", options=["Linear", "Log"], index=1)

    plot_histo(
        summary_df,
        ddl_command="ALL",
        experiment_name=f"All {experiment_name}",
        y_axis_type=y_axis_type,
        series_column="system_name",
        additional_column="ddl_command",
        legend_title="System | DDL Command",
    )

    plot_histo(
        create_summary_df,
        ddl_command="CREATE",
        experiment_name=f"BATCHED CREATE with {experiment_name}",
        y_axis_type=y_axis_type,
        series_column="system_name",
        legend_title="CREATE: System, Object Type",
    )
    plot_histo(
        alter_summary_df,
        ddl_command="ALTER",
        experiment_name=f"BATCHED CREATE with {experiment_name}",
        y_axis_type=y_axis_type,
        series_column="system_name",
        legend_title="ALTER: System, Object Type",
    )

    plot_histo(
        comment_summary_df,
        ddl_command="COMMENT",
        experiment_name=f"BATCHED CREATE with {experiment_name}",
        y_axis_type=y_axis_type,
        series_column="system_name",
        legend_title="COMMENT: System, Object Type",
    )

    plot_histo(
        show_summary_df,
        ddl_command="SHOW",
        experiment_name=f"BATCHED CREATE with {experiment_name}",
        y_axis_type=y_axis_type,
        series_column="system_name",
        legend_title="SHOW: System, Object Type",
    )


def chunked_avg_runtime(data_df, chunk_size=50, columns=["system_name", "ddl_command", "target_object"]):
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


def plot_summary(
    data_df,
    experiment_name,
    ddl_command,
    y_axis_type,
    y="avg_runtime",
    series_column="ddl_command",
    legend_title="DDL Command",
    legend_orientation="h",
    line_dash=None,
    markers: bool = False,
    symbol=None,
    log_x=False,
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
    st.subheader(f"Avg runtime: {experiment_name} - {ddl_command}")

    plot_dataframe(data_df, "Query Data")

    data_df["system_label"] = data_df["system_name"].map(OPENDICT_LABELS)

    fig = px.line(
        data_df,
        x="granularity",
        y=y,
        color=series_column,
        line_dash=line_dash,
        markers=markers,
        symbol=symbol,
        labels={
            "target_object": "Target Object",
            "avg_runtime": "Avg. Runtime (s)",
            "granularity": "Objects in Metastore",
            "ddl_command": "DDL Command",
            "system_name": "System Name",
            "cumulative_runtime": "Cumulative Runtime",
        },
        color_discrete_map=SYSTEM_LABEL_COLOR_MAP,
        category_orders={"system_name": SYSTEM_ORDER, "system_label": SYSTEM_LABEL_ORDER},
        log_y=(y_axis_type == "Log"),  # Apply log scale to y-axis if selected
        log_x=(y_axis_type == "Log") if log_x else False,
    )

    fig.update_traces(marker=dict(size=10))

    fig.update_layout(
        legend_title=legend_title,
        template="plotly_white",
        yaxis=dict(title="Avg. Runtime (s)", exponentformat="none"),
        legend=dict(
            orientation=legend_orientation,
            xanchor="center",  # anchor at center
            yanchor="bottom",  # anchor on bottom of text
            font=dict(size=LEGEND_FONT_SIZE),
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
    st.subheader(f"Average CREATE Query Runtime by Object & Objects in Metastore for {experiment_name.capitalize()}")

    plot_dataframe(data_df, "Query Data")

    data_df["system_label"] = data_df["system_name"].map(OPENDICT_LABELS)

    fig = px.line(
        data_df,
        x="granularity",
        y="avg_runtime",
        color="system_name",
        labels={
            "target_object": "Target Object",
            "avg_runtime": "Avg. Runtime (s)",
            "granularity": "Objects in Metastore",
            "ddl_command": "DDL Command",
            "system_name": "System Name",
        },
        line_dash="target_object",
        color_discrete_map=SYSTEM_LABEL_COLOR_MAP,
        category_orders={"system_name": SYSTEM_ORDER},
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
            font=dict(size=LEGEND_FONT_SIZE),
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

    plot_dataframe(data_df, "Query Data")

    data_df["system_label"] = data_df["system_name"].map(OPENDICT_LABELS)

    fig = px.line(
        data_df,
        x="granularity",
        y="avg_runtime",
        color="system_name",
        color_discrete_map=SYSTEM_LABEL_COLOR_MAP,
        category_orders={"system_name": SYSTEM_ORDER, "system_label": SYSTEM_LABEL_ORDER},
        labels={
            "target_object": "Target Object",
            "avg_runtime": "Avg. Runtime (s)",
            "granularity": "Objects in Metastore",
            "ddl_command": "DDL Command",
            "system_name": "System Name",
        },
        line_dash="target_object",
        log_y=(y_axis_type == "Log"),  # Apply log scale if selectedm
        log_x=(y_axis_type == "Log"),  # Apply log scale if selected
        symbol="system_name",
    )

    fig.update_layout(
        legend=dict(
            orientation="h",
            xanchor="center",  # anchor at center
            yanchor="bottom",  # anchor on bottom of text
            x=0.5,  # horizontal center
            y=1.0,  # just above the plotting area
            font=dict(size=LEGEND_FONT_SIZE),
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

    plot_dataframe(data_df, "Query Data")

    data_df["system_label"] = data_df["system_name"].map(OPENDICT_LABELS)

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
            "granularity": "Objects in Metastore",
            "ddl_command": "DDL Command",
            "system_name": "System Name",
        },
        color_discrete_map=SYSTEM_LABEL_COLOR_MAP,
        category_orders={"system_name": SYSTEM_ORDER, "system_label": SYSTEM_LABEL_ORDER},
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
            font=dict(size=LEGEND_FONT_SIZE),
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


def create_tldr_dashboard(category_map: dict[str, str], conn: duckdb.DuckDBPyConnection):
    y_axis_type = st.sidebar.selectbox("Y-axis scale", options=["Linear", "Log"], index=1)

    plot_001_histo_experiment_total_runtime(conn)
    plot_002_all_create_dashboard(conn, y_axis_type=y_axis_type)
    plot_004_storage(data_df=storage_data.df_storage, y_axis_type=y_axis_type)
    plot_003_all_alter_commet_show(conn=conn, y_axis_type=y_axis_type)
    # plot_005_opendic_optimization_overview(conn=conn, y_axis_type=y_axis_type)
    plot_006_opendic_optimization_overview(conn=conn, y_axis_type=y_axis_type)


def plot_006_opendic_optimization_overview(conn, y_axis_type):
    chunk_size = 50
    query = f"""
    WITH avg_by_granularity AS (
        -- First average runtimes for entries with the same granularity
        SELECT
            system_name,
            ddl_command,
            granularity,
            AVG(query_runtime) AS avg_runtime
        FROM {ALL_DATA_VIEW}
        WHERE ddl_command = 'CREATE'
            AND LOWER(system_name) LIKE 'opendict%'
            AND LOWER(system_name) NOT LIKE '%batch%'
            AND LOWER(system_name) NOT LIKE '%cloud%'
        GROUP BY system_name, ddl_command, granularity
    ),
    chunked_data AS (
        -- Then apply chunking to the averaged data
        SELECT
            system_name,
            ddl_command,
            granularity,
            avg_runtime,
            CAST((ROW_NUMBER() OVER (ORDER BY system_name, ddl_command, granularity) - 1) / {chunk_size} AS INTEGER) AS chunk_id
        FROM avg_by_granularity
    )
    -- Finally, average within chunks
    SELECT
        system_name,
        ddl_command,
        chunk_id,
        AVG(avg_runtime) AS avg_runtime,
        FIRST(granularity) AS granularity
    FROM chunked_data
    GROUP BY system_name, ddl_command, chunk_id
    ORDER BY chunk_id ASC, granularity ASC;
    """
    create_df = conn.execute(query).fetch_df()

    plot_summary(
        create_df,
        ddl_command="CREATE",
        experiment_name="CUMULATIVE",
        y_axis_type=y_axis_type,
        series_column="system_label",
    )


# def plot_005_opendic_optimization_overview(conn, y_axis_type):
#     opendic_batch_create_query = f"""
#         WITH avg_by_granularity AS (
#             -- First average runtimes for entries with the same granularity
#             SELECT
#                 system_name,
#                 ddl_command,
#                 granularity,
#                 AVG(query_runtime) AS avg_runtime
#             FROM {ALL_DATA_VIEW}
#             WHERE ddl_command = 'CREATE'
#                 AND LOWER(system_name) LIKE '%batch%'
#             GROUP BY system_name, ddl_command, granularity
#         )
#         -- Calculate cumulative sum of averages
#         SELECT
#             system_name,
#             ddl_command,
#             granularity,
#             SUM(ANY_VALUE(avg_runtime)) OVER (PARTITION BY system_name, ddl_command ORDER BY granularity) as cumulative_runtime
#         FROM avg_by_granularity
#         GROUP BY system_name, ddl_command, granularity
#         ORDER BY granularity ASC;
#         """

#     chunk_size = 50
#     opendic_create_query = f"""
#     WITH avg_by_granularity AS (
#         -- First average runtimes for entries with the same granularity
#         SELECT
#             system_name,
#             ddl_command,
#             granularity,
#             AVG(query_runtime) AS avg_runtime
#         FROM {ALL_DATA_VIEW}
#         WHERE ddl_command = 'CREATE'
#             AND LOWER(system_name) LIKE 'opendict%'
#             AND LOWER(system_name) NOT LIKE '%batch'
#         GROUP BY system_name, ddl_command, granularity
#     ),
#     chunked_data AS (
#         -- Then apply chunking to the averaged data
#         SELECT
#             system_name,
#             ddl_command,
#             granularity,
#             avg_runtime,
#             CAST((ROW_NUMBER() OVER (ORDER BY system_name, ddl_command, granularity) - 1) / {chunk_size} AS INTEGER) AS chunk_id
#         FROM avg_by_granularity
#     )
#     -- Calculate cumulative runtime by chunk
#     SELECT
#         system_name,
#         ddl_command,
#         chunk_id,
#         SUM(ANY_VALUE(avg_runtime)) OVER (PARTITION BY system_name, ddl_command ORDER BY chunk_id) AS cumulative_runtime,
#         FIRST(granularity) AS granularity
#     FROM chunked_data
#     GROUP BY system_name, ddl_command, chunk_id
#     ORDER BY chunk_id ASC;
#     """

#     opendic_create_df = conn.execute(opendic_create_query).fetch_df()
#     opendic_batch_create_df = conn.execute(opendic_batch_create_query).fetch_df()

#     # Rename the runtime column to have consistent column names
#     opendic_create_df = opendic_create_df.rename(columns={"cumulative_runtime": "cumulative_runtime"})
#     opendic_batch_create_df = opendic_batch_create_df.rename(columns={"cumulative_runtime": "cumulative_runtime"})

#     create_df = pd.concat([opendic_create_df, opendic_batch_create_df])

#     plot_summary(
#         create_df,
#         ddl_command="CREATE",
#         experiment_name="CUMULATIVE",
#         y_axis_type=y_axis_type,
#         series_column="system_label",
#         y="cumulative_runtime",  # Specify the new column to use for y-axis
#         log_x=y_axis_type == "Log",
#     )


def plot_004_storage(data_df, y_axis_type: str):
    # Display the raw data
    plot_dataframe(data_df, "Show raw data")

    DISPLAY_ORDER = ["sqlite", "duckdb", "postgres", "opendict (local)", "opendict (cloud)", "opendict (cloud)"]

    fig_storage = px.bar(
        data_df,
        x="system_label",
        y="Storage Usage (GB)",
        color="system_label",
        title="Storage Usage by Datasystem System (GB)",
        log_y=(y_axis_type == "Log"),
        labels={
            "system_label": "Data System",
            "Storage Usage (GB)": "Storage Usage (GB)",
            "Metadatafiles Count": "Metadatafiles",
            "Datafiles Count": "Datafiles",
        },
        hover_data={
            "system_label": True,
            "Storage Usage (GB)": True,
            "Metadatafiles Count": True,
            "Datafiles Count": True,
        },
        color_discrete_map=SYSTEM_LABEL_COLOR_MAP,
        category_orders={"system_label": DISPLAY_ORDER},
        text="Storage Usage (GB)",
    )

    fig_storage.update_traces(
        texttemplate="%{text:.2f} GB.",  # format the label
        textposition="auto",  # keep labels inside the bars
        cliponaxis=True,  # disable clipping so labels stay on zoom
    )

    fig_storage.update_layout(
        xaxis_title="",
        yaxis_title="Storage Usage (GB)",
        showlegend=False,
        # legend=dict(
        #     orientation="h",
        #     x=0.5,  # horizontal center
        #     xanchor="center",
        #     y=1.0,  # just above the plotting area
        #     yanchor="bottom",
        # ),
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


def plot_003_all_alter_commet_show(conn: duckdb.DuckDBPyConnection, y_axis_type: str):
    alter_summary_df = get_all_alter_data(conn)
    comment_summary_df = get_all_comment_data(conn)
    show_summary_df = get_all_show_data(conn)

    plot_summary(
        alter_summary_df,
        ddl_command="ALTER",
        experiment_name="ALL",
        y_axis_type=y_axis_type,
        series_column="system_label",
        legend_title="System Name",
        log_x=True,
        symbol="system_label",
    )

    plot_summary(
        comment_summary_df,
        ddl_command="COMMENT",
        experiment_name="ALL",
        y_axis_type=y_axis_type,
        series_column="system_label",
        legend_title="System Name",
        log_x=True,
        symbol="system_label",
    )

    plot_summary(
        show_summary_df,
        ddl_command="SHOW",
        experiment_name="ALL",
        y_axis_type=y_axis_type,
        series_column="system_label",
        legend_title="System Name",
        log_x=True,
        symbol="system_label",
    )


def plot_002_all_create_dashboard(conn: duckdb.DuckDBPyConnection, y_axis_type: str):
    chunk_size = 50
    query = f"""
    WITH avg_by_granularity AS (
        -- First average runtimes for entries with the same granularity
        SELECT
            system_name,
            ddl_command,
            granularity,
            AVG(query_runtime) AS avg_runtime
        FROM {ALL_DATA_VIEW}
        WHERE ddl_command = 'CREATE'
            AND LOWER(system_name) NOT LIKE '%batch%'
            AND LOWER(system_name) NOT LIKE 'opendict_polaris_file'
            -- AND LOWER(system_name) NOT LIKE 'opendict_polaris_cloud%'
        GROUP BY system_name, ddl_command, granularity
    ),
    chunked_data AS (
        -- Then apply chunking to the averaged data
        SELECT
            system_name,
            ddl_command,
            granularity,
            avg_runtime,
            CAST((ROW_NUMBER() OVER (ORDER BY system_name, ddl_command, granularity) - 1) / {chunk_size} AS INTEGER) AS chunk_id
        FROM avg_by_granularity
    )
    -- Finally, average within chunks
    SELECT
        system_name,
        ddl_command,
        chunk_id,
        AVG(avg_runtime) AS avg_runtime,
        FIRST(granularity) AS granularity
    FROM chunked_data
    GROUP BY system_name, ddl_command, chunk_id
    ORDER BY chunk_id ASC, granularity ASC;
    """

    create_df = conn.execute(query).fetch_df()

    plot_summary(
        create_df,
        ddl_command="CREATE",
        experiment_name="ALL",
        y_axis_type=y_axis_type,
        series_column="system_label",
    )


def plot_001_histo_experiment_total_runtime(conn: duckdb.DuckDBPyConnection):
    """
    Plots the total runtime for each experiment/database as a horizontal bar chart.

    Args:
        data_df (pd.DataFrame): Dataframe containing the benchmark data.
    """
    st.subheader("Total Runtime by Experiment/Database")

    # Sum the average runtimes for each system to get total runtime
    query = f"""
    WITH avg_runtime AS (
      SELECT
        system_name,
        ddl_command,
        granularity,
        AVG(query_runtime) AS avg_runtime
      FROM
        {ALL_DATA_VIEW}
      GROUP BY
        system_name,
        ddl_command,
        granularity
    )
    SELECT
      system_name,
      SUM(avg_runtime) AS total_runtime
    FROM
      avg_runtime
    GROUP BY
      system_name
    ORDER BY
      total_runtime ASC;

    """
    total_runtime_df = conn.execute(query).df()
    total_runtime_df["total_runtime"] = total_runtime_df["total_runtime"] / 60 / 60  # Convert to hours

    plot_dataframe(total_runtime_df, "View Raw Runtime Data")

    total_runtime_df["system_label"] = total_runtime_df["system_name"].map(OPENDICT_LABELS)

    # Create horizontal bar chart
    fig = px.bar(
        total_runtime_df,
        y="system_label",
        x="total_runtime",
        orientation="h",
        # log_x= True,
        labels={"system_name": "Database/Experiment", "total_runtime": "Total Runtime (hours)"},
        color="system_label",  # Color bars by system name
        color_discrete_map=SYSTEM_LABEL_COLOR_MAP,
        category_orders={"system_name": SYSTEM_ORDER, "system_label": SYSTEM_LABEL_ORDER},
        text="total_runtime",  # specify the column to show as labels
    )
    # tick_vals = list(OPENDICT_LABELS.keys())
    # tick_text = list(OPENDICT_LABELS.values())

    fig.update_layout(
        showlegend=False,
        xaxis=dict(title="Total Runtime (hours)"),
        # yaxis=dict(tickmode="array", tickvals=tick_vals, ticktext=tick_text, showticklabels=False ),
        # yaxis=dict(tickmode="array", tickvals=tick_vals, ticktext=tick_text),
        yaxis_title="",
    )

    fig.update_traces(
        texttemplate="%{text:.3f} h.",  # format the label
        textposition="auto",  # keep labels inside the bars
        cliponaxis=True,  # disable clipping so labels stay on zoom
        insidetextanchor="start",  # Anchors the text well within the bar
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
    category_map = {"Standard": "data/standard/", "Opendic": "data/opendic/", "Opendic_batch": "data/opendic_batch/"}
    with load_all_data() as conn:
        if sidebar_category != "TLDR":
            create_dashboard(category_map[sidebar_category], conn)
        else:
            create_tldr_dashboard(category_map, conn)
