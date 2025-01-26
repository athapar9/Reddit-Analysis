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

def rank_colors_by_distinct_users(final_parquet, start, end):
    color_mapping = {
        "#FFFFFF": "White",
        "#00CCC0": "Cyan",
        "#94B3FF": "Very light blue",
        "#6A5CFF": "Light blue",
        "#E4ABFF": "Pale Violet",
        "#009EAA": "Dark Cyan",
        "#515252": "Very dark grayish cyan",
        "#00CC78": "Lime green",
        "#D4D7D9": "Light grayish blue",
        "#000000": "Black",
        "#2450A4": "Dark blue",
        "#3690EA": "Bright blue",
        "#FF3881": "Light pink",
        "#6D001A": "Very dark red",
        "#493AC1": "Moderate blue",
        "#FFB470": "Very light orange",
        "#898D90": "Dark grayish blue",
        "#DE107F": "Vivid pink",
        "#FFD635": "Light yellow",
        "#FFF8B8": "Pale yellow",
        "#FF4500": "Orange",
        "#E4ABFF": "Pale violet",
        "#51E9F4": "Soft cyan",
        "#811E9F": "Dark magenta",
        "#00A368": "Dark cyan 2",
        "#7EED56": "Soft green",
        "#9C6926": "Brown",
        "#FF99AA": "Very light red",
        "#B44AC0": "Magenta",
        "#BE0039": "Strong pink",
        "#00756F": "Dark cyan 2",
        "#FFA800": "Orange 2",
        "#6D482F": "Dark orange",
    }

    query = f"""
    SELECT
        pixel_color,
        COUNT(DISTINCT user_id_hashed) AS distinct_users
    FROM {final_parquet}
    WHERE timestamp >= '{start}' AND timestamp <= '{end}'
    GROUP BY pixel_color
    ORDER BY distinct_users DESC;
    """
    results = duckdb.query(query).to_df()
    results["pixel_color"] = results["pixel_color"].map(color_mapping).fillna("Unknown")
    return results

def count_first_time_users(final_parquet, start, end):
    query = f"""
    SELECT COUNT(*)
    FROM (
        SELECT user_id_hashed, MIN(timestamp) AS first_pixel_time
        FROM {final_parquet}
        GROUP BY user_id_hashed
        HAVING first_pixel_time >= '{start}' AND first_pixel_time <= '{end}'
    );
    """
    result = duckdb.query(query).fetchone()
    return result[0]

def pixel_placement_percentiles(final_parquet, start, end):
    query = f"""
    SELECT user_id_hashed, COUNT(*) AS pixel_count
    FROM {final_parquet}
    WHERE timestamp >= '{start}' AND timestamp <= '{end}'
    GROUP BY user_id_hashed
    """
    results = duckdb.query(query).to_df()
    percentiles = results['pixel_count'].quantile([0.50, 0.75, 0.90, 0.99])
    return percentiles

def calculate_average_session_length(final_parquet, start, end):
    # Define a session as a user’s activity within a 15-minute window of inactivity. 
    # Return the average session length in seconds during the specified timeframe.
    # Only include cases where a user had more than one pixel placement during the time period in the average.

    # first separate each user's activity 
    # then define the 15-minute window of inacitivity
    # then create an id for each session of inactivity
    # calculate duration of sessions (exclude one piexel activity)
    # average the session length 
    
    query = f"""
    WITH user_activity AS (
        SELECT 
            user_id_hashed,
            timestamp,
            LAG(timestamp) OVER (PARTITION BY user_id_hashed ORDER BY timestamp) AS previous_timestamp
        FROM read_parquet({final_parquet})
        WHERE timestamp >= '{start}' AND timestamp <= '{end}'
    ),
    sessions AS (
        SELECT 
            user_id_hashed,
            timestamp,
            CASE 
                WHEN previous_timestamp IS NULL OR EXTRACT(EPOCH FROM (timestamp - previous_timestamp)) > 900 THEN 1 ELSE 0 
            END AS new_session
        FROM user_activity
    ),
    session_assignments AS (
        SELECT 
            user_id_hashed,
            timestamp,
            SUM(new_session) OVER (PARTITION BY user_id_hashed ORDER BY timestamp) AS session_id
        FROM sessions
    ),
    session_durations AS (
        SELECT 
            user_id_hashed,
            session_id,
            MAX(timestamp) - MIN(timestamp) AS session_length
        FROM session_assignments
        GROUP BY user_id_hashed, session_id
        HAVING COUNT(*) > 1
    )
    SELECT 
        AVG(EXTRACT(EPOCH FROM session_length)) AS avg_session_length
    FROM session_durations;
    """
    result = duckdb.query(query).fetchone()
    avg_session_length = result[0]
    return avg_session_length

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
    # preprocess(original_csv, preprocessed_data_path)

    start_perf = time.perf_counter_ns()
    rank_colors_by_distinct_users_result = rank_colors_by_distinct_users('final.parquet', start, end)
    count_first_time_users_result = count_first_time_users('final.parquet', start, end)
    average_session_length_result = calculate_average_session_length('final.parquet', start, end)
    pixel_placement_percentiles_result = pixel_placement_percentiles('final.parquet', start, end)
    end_perf = time.perf_counter_ns()
    runtime = (end_perf - start_perf) // 1_000_000

    print("### Ranking of Colors by Distinct Users")
    print("- **Top**")
    for index, row in rank_colors_by_distinct_users_result.iterrows():
        print(f"  {index + 1}. {row['pixel_color']}: {row['distinct_users']} users")
    print(f"### Average Session Length")
    print(f"- **Average session length**: {average_session_length_result:.2f} seconds")
    print("### Percentiles of Pixels Placed")
    print(f"- **50th Percentile**: {int(pixel_placement_percentiles_result[0.50])} pixels")
    print(f"- **75th Percentile**: {int(pixel_placement_percentiles_result[0.75])} pixels")
    print(f"- **90th Percentile**: {int(pixel_placement_percentiles_result[0.90])} pixels")
    print(f"- **99th Percentile**: {int(pixel_placement_percentiles_result[0.99])} pixels")
    print("### Count of First-Time Users")
    print("- **Output:**", count_first_time_users_result, "users")
    print("### Runtime", runtime,  "ms")


if __name__ == "__main__":
    main()
