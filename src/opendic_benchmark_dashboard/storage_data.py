import pandas as pd

# Data preparation
db_systems = [
    # "opendict (no cleanup)",
    "opendict (local)",
    "opendict (local, batched)",
    "opendict (cloud)",
    "opendict (local, cached)",
    "duckdb",
    "sqlite",
]

# Convert all storage to GB for consistency
storage_data = [
    # 105.39 * (100_000 / 16_552),  # Opendict: 105.39 GB
    28.92,  # Standard: 28.92 GB
    10.08 / 1000,  # to GB
    12.8,  # cloud
    38.55,  # cache
    1.24,  # DuckDB: 1.24 GB
    0.419,  # SQLite: 0.419 GB
]

datafiles = [
    # 16928 * (100_000 / 16_552),  # Opendict
    1298,  # Standard
    82,  # In-mem Cache
    int(552_4600 * (1298 / (1298 + 119484))),
    1655,
    1,  # DuckDB (100% represented as 1 for visualization)
    1,  # SQLite (100% represented as 1 for visualization)
]

metadatafiles = [
    # 67995 * (100_000 / 16_552),  # Opendict
    119484,  # Standard
    361,  # In-mem Cache
    int(552_4600 * (119484 / (1298 + 119484))),
    120503,
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
