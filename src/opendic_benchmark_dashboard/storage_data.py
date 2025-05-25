import pandas as pd

# Data preparation
db_systems = [
    # "opendict (no cleanup)",
    "opendict (local)",
    "opendict (local, batch)",
    "opendict (cloud)",
    # "opendict (local, cached)",
    "duckdb",
    "sqlite",
    "postgres",
]

# Convert all storage to GB for consistency
storage_data = [
    # 105.39 * (100_000 / 16_552),  # Opendict: 105.39 GB
    28.92,  # Standard: 28.92 GB
    10.08 / 1000,  # to GB
    12.8,  # cloud
    # 38.55,  # cache
    1.24,  # DuckDB: 1.24 GB
    0.419,  # SQLite: 0.419 GB
    3.54,
]

datafiles = [
    # 16928 * (100_000 / 16_552),  # Opendict
    1298,  # Standard
    82,  # In-mem Cache
    int(552_4600 * (1298 / (1298 + 119484))),
    # 1655,
    1,  # DuckDB (100% represented as 1 for visualization)
    1,  # SQLite (100% represented as 1 for visualization)
    0,
]

metadatafiles = [
    # 67995 * (100_000 / 16_552),  # Opendict
    119484,  # Standard
    361,  # In-mem Cache
    int(552_4600 * (119484 / (1298 + 119484))),
    # 120503,
    0,  # DuckDB (no separate metadata files mentioned)
    0,  # SQLite (no separate metadata files mentioned)
    0,
]

# Create a DataFrame for easier manipulation
df_storage = pd.DataFrame(
    {
        "system_label": db_systems,
        "Storage Usage (GB)": storage_data,
        "Datafiles Count": datafiles,
        "Metadatafiles Count": metadatafiles,
    }
)

maintenance_labels = [
    "opendict (no-maintenance)",
    "opendict (snapshot-expiry)",
    "opendict (snapshot-expiry, metadata-cleanup)",
    "opendict batch (snapshot-expiry)",
    "opendict batch (snapshot-expiry, metadata-cleanup)",
]
# Convert all storage to GB for consistency
maintenance_storage_data = [
    105.39 * (100_000 / 16_552),  # no-maintenance: 105.39 GB
    28.92,  # (snapshot-expiry)
    143.3 / 1000,  # (snapshot-expiry, metadata-cleanup)
    13.5 / 1000,  # to GB. Batch. snapshot-expiry
    13.5 / 1000,  # to GB. (snapshot-expiry, metadata-cleanup)
]

maintenance_datafiles = [
    16928 * (100_000 / 16_552),  # no-maintenance
    1298,  # (snapshot-expiry)
    -1, # (snapshot-expiry, metadata-cleanup)
    82,  # batch. (snapshot-expiry)
    82,  # batch. (snapshot-expiry, snapshot-expiry, metadata-cleanup))
    # 1655,
]

maintenance_metadatafiles = [
    -1,  # Opendict
    119484,  # (snapshot-expiry)
    4826 ,    # (snapshot-expiry, metadata-cleanup)
    361,  # # batch. (snapshot-expiry)
    334,  # (snapshot-expiry, snapshot-expiry, metadata-cleanup),
]


df_storage_maintenance = pd.DataFrame(
    {
        "system_label": maintenance_labels,
        "Storage Usage (GB)": maintenance_storage_data,
        "Datafiles Count": maintenance_datafiles,
        "Metadatafiles Count": maintenance_metadatafiles,
    }
)
