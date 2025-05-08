import pandas as pd

# Data preparation
db_systems = ["Opendict (No cleanup)", "Opendict", "Opendict (batched)", "DuckDB", "SQLite"]

# Convert all storage to GB for consistency
storage_data = [
    105.39,  # Opendict: 105.39 GB
    28.92,  # Standard: 28.92 GB
    10.08 / 1000,  # to GB
    1.24,  # DuckDB: 1.24 GB
    0.419,  # SQLite: 0.419 GB
]

datafiles = [
    16928,  # Opendict
    1298,  # Standard
    82,  # In-mem Cache
    1,  # DuckDB (100% represented as 1 for visualization)
    1,  # SQLite (100% represented as 1 for visualization)
]

metadatafiles = [
    67995,  # Opendict
    119484,  # Standard
    361,  # In-mem Cache
    0,  # DuckDB (no separate metadata files mentioned)
    0,  # SQLite (no separate metadata files mentioned)
]

# Create a DataFrame for easier manipulation
df_storage = pd.DataFrame(
    {
        "Database System": db_systems,
        "Storage Usage (GB)": storage_data,
        "Datafiles Count": datafiles,
        "Metadatafiles Count": metadatafiles,
    }
)
