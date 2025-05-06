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
tab_standard, tab_opendic = st.tabs(["Standard", "Opendic"])


def create_dashboard(tab, data_dir):
    with tab:
        # Get list of available database files
        data_files = [f for f in os.listdir(data_dir) if f.endswith(".parquet")]
        database_options = sorted([os.path.splitext(f)[0] for f in data_files])

        # Sidebar for controls
        st.sidebar.header("Dashboard Controls")
        selected_db = st.sidebar.selectbox("Select Database", options=database_options)

        # Load data
        data_file = f"{data_dir}{selected_db}.parquet"
        data_df = pd.read_parquet(data_file, engine="fastparquet")

        # Display raw data in expandable section
        with st.expander("View Raw Data"):
            st.dataframe(data_df)

        # Filter for 'CREATE' commands and average runtimes
        create_summary = (
            data_df[data_df["ddl_command"] == "CREATE"]
            .groupby(["ddl_command", "target_object", "granularity"], as_index=False)
            .agg(avg_runtime=(("query_runtime"), "mean"))
        )

        create_summary_compacted = create_summary.iloc[::20]

        # Create visualization for CREATE commands
        st.subheader(f"Average CREATE Query Runtime by Object & Granularity for {selected_db.capitalize()}")
        fig = px.line(
            create_summary_compacted,
            x="granularity",
            y="avg_runtime",
            color="target_object",
            labels={"target_object": "Target Object", "avg_runtime": "Avg. Runtime (s)", "granularity": "Granularity"},
        )

        fig.update_layout(
            xaxis_tickangle=-45, yaxis=dict(title="Avg. Runtime (s)"), legend_title="Granularity", template="plotly_white"
        )

        st.plotly_chart(fig, use_container_width=True)

        # Additional visualizations
        st.subheader("Alter plots")


opendic_dir = "data/opendic/"
standard_dir = "data/standard/"
create_dashboard(tab_standard, standard_dir)
create_dashboard(tab_opendic, opendic_dir)
