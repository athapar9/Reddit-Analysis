import sys
import os
import time
from datetime import datetime
import duckdb
import polars as pl
import numpy as np

def validate_args(start_time, end_time):
    """Ensure your script validates that the end hour is after the start hour."""
    format = "%Y-%m-%d %H"
    try:
        start = datetime.strptime(start_time, format)
        end = datetime.strptime(end_time, format)
        if end <= start:
            raise ValueError("End time must be after start time.")
        return start, end
    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)

def preprocess(original_csv, preprocessed_data):
    pl.scan_csv(original_csv).sink_parquet(preprocessed_data, compression="zstd", compression_level=22)
    split_coordinates_parquet = split_coordinates(preprocessed_data)
    user_id_parquet = convert_user_ids(split_coordinates_parquet)
    final_parquet = cast_timestamp(user_id_parquet)

def split_coordinates(preprocessed_data):
    duckdb.query('PRAGMA max_temp_directory_size="100GiB";')

    query = f"""
    COPY (
        SELECT 
            timestamp, 
            user_id, 
            pixel_color,
            CAST(SPLIT_PART(coordinate, ',', 1) AS INT) AS x, 
            CAST(SPLIT_PART(coordinate, ',', 2) AS INT) AS y
        FROM {preprocessed_data}
    ) TO 'split_coordinates.parquet' 
    (FORMAT 'parquet', COMPRESSION 'zstd', COMPRESSION_LEVEL 22);
    """

    duckdb.query(query)
    os.remove(preprocessed_data)
    print(f"Compressed coordinates saved to split_coordinates.parquet")
    return 'split_coordinates.parquet'

def convert_user_ids(split_coordinates_parquet):
    duckdb.query('PRAGMA max_temp_directory_size="100GiB";')
    query = f"""
    COPY (
        SELECT 
            timestamp, 
            pixel_color,
            x,
            y,
            hash(user_id) AS user_id_hashed
        FROM {split_coordinates_parquet}
    ) TO 'user_ids.parquet' 
    (FORMAT 'parquet', COMPRESSION 'zstd', COMPRESSION_LEVEL 22);
    """
    duckdb.query(query)
    os.remove(split_coordinates_parquet)
    print(f"User ID mapping saved to user_ids.parquet")
    return 'user_ids.parquet'

def cast_timestamp(user_ids_parquet):
    duckdb.query('PRAGMA max_temp_directory_size="100GiB";')
    query = f"""
    COPY (
        SELECT 
            CAST(timestamp AS TIMESTAMP) AS timestamp, 
            user_id_hashed, 
            pixel_color,
            x,
            y
        FROM {user_ids_parquet}
    ) TO 'final.parquet' 
    (FORMAT 'parquet', COMPRESSION 'zstd', COMPRESSION_LEVEL 22);
    """
    duckdb.query(query)
    os.remove(user_ids_parquet)
    print(f"Timestamps have been cast and saved to final.parquet")
    return 'final.parquet'

def main():
    if len(sys.argv) != 5:
        print("Usage: python3 script.py <start_date> <start_hour> <end_date> <end_hour>")
        sys.exit(1)

    start_time = sys.argv[1] + " " + sys.argv[2]
    end_time = sys.argv[3] + " " + sys.argv[4]
    validate_args(start_time, end_time)
    start = sys.argv[1] + " " + sys.argv[2] + ":00"
    end = sys.argv[3] + " " + sys.argv[4] + ":00"

    original_csv = "../2022_place_canvas_history.csv"
    preprocessed_data_path = "preprocessed_place.parquet"
    preprocess(original_csv, preprocessed_data_path)

if __name__ == "__main__":
    main()
