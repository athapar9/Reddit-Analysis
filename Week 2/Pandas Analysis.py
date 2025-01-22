import sys
import time
import pandas as pd
from datetime import datetime, timedelta, timezone

def validate_args(start_time, end_time):
    time_format = "%Y-%m-%d %H"
    try:
        start = datetime.strptime(start_time, time_format).replace(tzinfo=timezone.utc)
        end = datetime.strptime(end_time, time_format).replace(tzinfo=timezone.utc)
        if start >= end:
            raise ValueError("Start time must be before end time.")
        return start, end
    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)

def analyze_data(csv_file_path, start, end):
    df = pd.read_csv(csv_file_path,  usecols=["timestamp", "pixel_color", "coordinate"], parse_dates=['timestamp'])
    df['timestamp'] = df['timestamp'].dt.tz_convert('UTC')
    filtered_df = df[(df['timestamp'] >= start) & (df['timestamp'] < end)]

    most_placed_color = filtered_df['pixel_color'].mode().iloc[0] 
    most_placed_pixel_location = filtered_df['coordinate'].mode().iloc[0] 

    return most_placed_color, most_placed_pixel_location

def create_results(results):
    with open('pandas_test_results.md', 'w') as results_file:
        results_file.write('# Analysis Results\n\n')
        for result in results:
            results_file.write(f"## {result['name']}\n")
            results_file.write(f"- **Timeframe:** {result['timeframe']}\n")
            results_file.write(f"- **Execution Time:** {result['execution_time']} ms\n")
            results_file.write(f"- **Most Placed Color:** {result['most_color']}\n")
            results_file.write(f"- **Most Placed Pixel Location:** {result['most_pixel']}\n\n")

def main():
    if len(sys.argv) != 5:
        print("Usage: python3 script.py <start_date> <start_hour> <end_date> <end_hour>")
        sys.exit(1)

    start_time = sys.argv[1] + " " + sys.argv[2]
    end_time = sys.argv[3] + " " + sys.argv[4]
    start, end = validate_args(start_time, end_time)

    csv_file_path = "../2022_place_canvas_history.csv"

    timeframes = [
        {"name": "1-Hour Timeframe", "start": start, "end": start + timedelta(hours=1)},
        {"name": "3-Hour Timeframe", "start": start, "end": start + timedelta(hours=3)},
        {"name": "6-Hour Timeframe", "start": start, "end": start + timedelta(hours=6)}
    ]

    results = []

    for timeframe in timeframes:
        start_perf = time.perf_counter_ns()

        most_placed_color, most_placed_pixel_location = analyze_data(csv_file_path, timeframe['start'], timeframe['end'])

        end_perf = time.perf_counter_ns()

        execution_time = (end_perf - start_perf) // 1_000_000

        results.append({
            "name": timeframe['name'],
            "timeframe": f"{timeframe['start']} to {timeframe['end']}",
            "execution_time": execution_time,
            "most_color": most_placed_color,
            "most_pixel": most_placed_pixel_location,
        })

    create_results(results)

if __name__ == "__main__":
    main()
