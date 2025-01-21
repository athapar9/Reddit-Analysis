import duckdb
import sys
import time
from datetime import datetime, timedelta

def validate_args(start_time, end_time):
    time_format = "%Y-%m-%d %H"
    try:
        start = datetime.strptime(start_time, time_format)
        end = datetime.strptime(end_time, time_format)
        if end <= start:
            raise ValueError("End time must be after start time.")
        return start, end
    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)

def analyze_data(start, end, csv_file_path):

    most_placed_color = duckdb.sql(f"""SELECT pixel_color, COUNT(*) AS count 
               FROM '{csv_file_path}'
               WHERE timestamp BETWEEN '{start}' AND '{end}'
               GROUP BY pixel_color 
               ORDER BY count DESC
               LIMIT 1""").fetchall()

    most_placed_pixel_location = duckdb.sql(f"""SELECT coordinate, COUNT(*) AS count 
               FROM '{csv_file_path}'
               WHERE timestamp BETWEEN '{start}' AND '{end}'
               GROUP BY coordinate 
               ORDER BY count DESC
               LIMIT 1""").fetchall()

    return most_placed_color[0][0], most_placed_pixel_location[0][0]

def create_results(results):
    with open('duckdb_test_results.md', 'w') as results_file:
        results_file.write('# Week 1 Results\n')
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

    timeframes = [
        {"name": "1-Hour Timeframe", "start": start, "end": start + timedelta(hours=1)},
        {"name": "3-Hour Timeframe", "start": start, "end": start + timedelta(hours=3)},
        {"name": "6-Hour Timeframe", "start": start, "end": start + timedelta(hours=6)}
    ]

    results = []
    csv_file_path = "../2022_place_canvas_history.csv"

    for timeframe in timeframes:
        start_perf = time.perf_counter_ns()

        most_placed_color, most_placed_pixel_location = analyze_data(timeframe['start'], timeframe['end'], csv_file_path)

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
