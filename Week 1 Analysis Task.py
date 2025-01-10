""" You will write a Python script that accepts a starting and ending hour as arguments and returns:
        The most placed color during that timeframe.
        The most placed pixel location during that timeframe.
         
    Input Format
        The script should accept start and end hours as command-line arguments in the following format:
        YYYY-MM-DD HH (e.g., 2022-04-01 12 for April 1, 2022, at 12:00 PM).
        Ensure your script validates that the end hour is after the start hour.

    Color Format
        The colors in the dataset are stored as hexadecimal codes (e.g., #FFFFFF for white). 
        You should return the most placed color in this format.
    
    Timing the Script
        Use Python's time.perf_counter_ns() or equivalent to measure execution time.

    Results Documentation
        In a markdown file named test_results_week_1.md, document the results of running your script with the following timeframes:

        A 1-hour timeframe.
        A 3-hour timeframe.
        A 6-hour timeframe.
        You can pick any timeframes as long as the timeframe is entirely within the results of the r/place data.

        For each timeframe, include:

        The selected timeframe.
        The number of milliseconds it took to compute the results.
        The output: the most placed color (in hex code) and the most placed pixel location (coordinates).
 """
import csv
import sys
import time
from datetime import datetime, timedelta
from collections import Counter

def parse_arguments():
    with open ('2022_place_canvas_history.csv', 'r') as file:
        csv_reader = csv.reader(file)
        header = next(csv_reader)

    print(header)

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

def parse_timestamp(timestamp, formats):
    """Parse a timestamp using multiple formats."""
    for fmt in formats:
        try:
            return datetime.strptime(timestamp, fmt)
        except ValueError:
            continue
    return None

def analyze_data(start, end):
    """Analyze the dataset for most placed color and pixel location."""
    most_placed_color = None
    most_placed_pixel_location = None
    color_counter = Counter()
    pixel_counter = Counter()
    formats = ["%Y-%m-%d %H:%M:%S.%f UTC", "%Y-%m-%d %H:%M:%S UTC"]

    with open("2022_place_canvas_history.csv", "r") as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            timestamp = parse_timestamp(row["timestamp"], formats)
            if not timestamp or not (start <= timestamp < end):
                continue
            color_counter[row["pixel_color"]] += 1
            pixel_counter[row["coordinate"]] += 1

    if color_counter:
        most_placed_color = color_counter.most_common(1)[0][0]
    if pixel_counter:
        most_placed_pixel_location = pixel_counter.most_common(1)[0][0]

    return most_placed_color, most_placed_pixel_location

def create_results(timeframes):
    with open('test_results_week_1.md', 'w+') as results_file:
        results_file.write('# Week 1 Results\n')
        results_file.write(f"## {timeframe['name']}\n")
        results_file.write(f"- **Timeframe:** {timeframe['timeframe']}\n")
        results_file.write(f"- **Execution Time:** {timeframe['execution_time']} ms\n")
        results_file.write(f"- **Most Placed Color:** {timeframe['most_color']}\n")
        results_file.write(f"- **Most Placed Pixel Location:** {timeframe['most_pixel']}\n\n")


def main():
    if len(sys.argv) != 5:
        print("Usage: python3 Week 1 Analysis Task.py <start_date> <start_hour> <end_date> <end_hour>")
        sys.exit(1)

    start_time = f"{sys.argv[1]} {sys.argv[2]}"
    end_time = f"{sys.argv[3]} {sys.argv[4]}"
    start, end = validate_args(start_time, end_time)

    timeframes = [
        {"name": "1-Hour Timeframe", "start": start, "end": start + timedelta(hours=1)},
        {"name": "3-Hour Timeframe", "start": start, "end": start + timedelta(hours=3)},
        {"name": "6-Hour Timeframe", "start": start, "end": start + timedelta(hours=6)}
    ]

    results = []

    for timeframe in timeframes:
        start_perf = time.perf_counter_ns()

        most_placed_color, most_placed_pixel_location = analyze_data(timeframe["start"], timeframe["end"])

        end_perf = time.perf_counter_ns()

        execution_time = (end_perf - start_perf) // 1_000_000

        results.append({
            "name": timeframe["name"],
            "timeframe": f"{timeframe['start']} to {timeframe['end']}",
            "execution_time": execution_time,
            "most_color": most_placed_color,
            "most_pixel": most_placed_pixel_location,
        })

    create_results(results)

if __name__ == "__main__":
    main()