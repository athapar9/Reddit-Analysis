import sys
import os
import time
from datetime import datetime
import duckdb
import polars as pl
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from PIL import Image

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

def top_painted_pixels(final_parquet):
    query = f"""
    SELECT 
        x, 
        y, 
        COUNT(*) AS pixel_count
    FROM read_parquet('{final_parquet}')
    GROUP BY x, y
    ORDER BY pixel_count DESC
    LIMIT 3;
    """
    results = duckdb.query(query).to_df()
    return results

def top_10_painted_pixels(final_parquet):
    query = f"""
    SELECT 
        x, 
        y, 
        COUNT(*) AS pixel_count
    FROM read_parquet('{final_parquet}')
    GROUP BY x, y
    ORDER BY pixel_count DESC
    LIMIT 10;
    """
    results = duckdb.query(query).to_df()
    return results

def top_colors_pixels(final_parquet):
    """Gets the top colors for the hardcoded pixels (0,0), (359,564), (349,564)."""
    query = f"""
    SELECT x, y, pixel_color, COUNT(*) AS Frequency
    FROM read_parquet('{final_parquet}')
    WHERE (x, y) IN ((0, 0), (359, 564), (349, 564))
    GROUP BY x, y, pixel_color
    ORDER BY x, y, Frequency DESC
    """
    results = duckdb.query(query).to_df()
    results['Color'] = results['pixel_color'].apply(lambda hex: color_mapping.get(hex, "Unknown"))
    return results

def plot_top_colors(top_colors_df):
    """Plots the top colors for the hardcoded pixels."""
    coords = top_colors_df[['x', 'y']].drop_duplicates()
    fig, axes = plt.subplots(1, len(coords), figsize=(6 * len(coords), 5))

    if len(coords) == 1:
        axes = [axes]

    for i, (x, y) in enumerate(coords.itertuples(index=False)):
        pixel_data = top_colors_df[(top_colors_df['x'] == x) & (top_colors_df['y'] == y)].head(3)
        axes[i].bar(pixel_data['Color'], pixel_data['Frequency'], color=pixel_data['pixel_color'], edgecolor="black", linewidth=1)
        axes[i].set_xlabel('Color')
        axes[i].set_ylabel('Frequency')
        axes[i].set_title(f'Top 3 Colors for ({x}, {y})')
        axes[i].tick_params(axis='x', rotation=45)

    plt.tight_layout()
    plt.savefig("top_colors_per_pixel.png", dpi=300, bbox_inches="tight")
    plt.show()

def hex_to_rgb(hex_color):
    hex_color = hex_color.lstrip('#')
    return tuple(int(hex_color[i:i+2], 16) for i in (0, 2, 4))

def regenerate_image(final_parquet, center_x, center_y, radius=50):
    
    query = f"""
    SELECT x, y, timestamp, pixel_color
    FROM read_parquet('{final_parquet}')
    WHERE x >= {center_x - radius} AND x <= {center_x + radius}
    AND y >= {center_y - radius} AND y <= {center_y + radius}
    AND timestamp >= '2022-04-01 00:00:00' AND timestamp < '2022-04-03 24:00:00'
    """
    
    pixel_data = duckdb.query(query).to_df()

    image_size = (2 * radius + 1, 2 * radius + 1)
    image = Image.new('RGB', image_size)

    for _, row in pixel_data.iterrows():
        x, y, hex_color = row['x'], row['y'], row['pixel_color']
        rgb_color = hex_to_rgb(hex_color)
        image.putpixel((x - (center_x - radius), y - (center_y - radius)), rgb_color)

    image.save(f"regenerated_image_{center_x}_{center_y}.png")
    image.show()

def main():
    final_parquet = 'final.parquet'
    top_pixels = top_painted_pixels(final_parquet)
    print("TOP PIXELS", top_pixels)
    top_colors = top_colors_pixels(final_parquet)
    print("TOP COLORS", top_colors)
    plot_top_colors(top_colors)
    regenerate_image(final_parquet, 349, 564)
    regenerate_image(final_parquet, 0, 0)
    top_10_pixels = top_10_painted_pixels(final_parquet)
    print("TOP 10 PIXELS", top_10_pixels)

if __name__ == "__main__":
    main()
