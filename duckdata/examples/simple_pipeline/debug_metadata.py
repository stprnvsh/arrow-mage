#!/usr/bin/env python3
"""
Debug script to examine the contents of the crosslink_metadata table
"""

import pandas as pd
import duckdb
import json
import os

# Connect to the database
db_path = os.path.join(os.path.dirname(__file__), "simple_pipeline.duckdb")
print(f"Connecting to database: {db_path}")
conn = duckdb.connect(db_path)

# List all tables
tables = conn.execute("SHOW TABLES").fetchall()
print("\nTables in database:")
for table in tables:
    print(f"- {table[0]}")

# Check if crosslink_metadata exists
if ('crosslink_metadata',) in tables:
    # Query metadata
    metadata_df = conn.execute("SELECT * FROM crosslink_metadata").fetchdf()
    print(f"\nFound {len(metadata_df)} rows in crosslink_metadata")
    
    # Print dataset names and tables
    print("\nDatasets:")
    for idx, row in metadata_df.iterrows():
        print(f"- {row['name']} (ID: {row['id']}, Table: {row['table_name']}, Language: {row['source_language']})")
    
    # Print full metadata for one dataset as an example
    if len(metadata_df) > 0:
        example = metadata_df.iloc[0]
        print("\nExample metadata for one dataset:")
        for col, val in example.items():
            if isinstance(val, str) and (col == 'schema' or col == 'arrow_schema' or col == 'access_languages' or col == 'memory_layout'):
                try:
                    # Try to parse JSON
                    parsed = json.loads(val)
                    print(f"  {col}: {json.dumps(parsed, indent=2, sort_keys=True)[:200]}...")
                except:
                    print(f"  {col}: {val[:100]}...")
            else:
                print(f"  {col}: {val}")
    
    # Check other related tables
    related_tables = ['crosslink_access_log', 'crosslink_schema_history', 'crosslink_lineage', 'crosslink_plasma_objects']
    for table in related_tables:
        if (table,) in tables:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            print(f"\nFound {count} rows in {table}")
            if count > 0:
                sample = conn.execute(f"SELECT * FROM {table} LIMIT 3").fetchdf()
                print(sample)
else:
    print("\nNo crosslink_metadata table found!")
    
    # Check if there's a different metadata table
    metadata_tables = [t[0] for t in tables if 'metadata' in t[0].lower()]
    if metadata_tables:
        print("Found possible metadata tables:")
        for table in metadata_tables:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            print(f"- {table}: {count} rows")
            if count > 0:
                cols = conn.execute(f"PRAGMA table_info({table})").fetchdf()
                print(f"  Columns: {', '.join(cols['name'])}")

# Check data tables
data_tables = [t[0] for t in tables if t[0].startswith('data_')]
print(f"\nData tables ({len(data_tables)}):")
for table in data_tables:
    count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    cols = conn.execute(f"SELECT * FROM {table} LIMIT 0").description
    col_names = [c[0] for c in cols]
    print(f"- {table}: {count} rows, columns: {', '.join(col_names)}")

# Close the connection
conn.close()
print("\nDebug complete.") 