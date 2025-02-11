import matplotlib.pyplot as plt
import pandas as pd
import duckdb
from PIL import Image

def hex_to_rgb(hex_color):
    hex_color = hex_color.lstrip('#')
    return tuple(int(hex_color[i:i+2], 16) for i in (0, 2, 4))

def top_users(final_parquet, limit):
    query = f"""
    SELECT user_id_hashed, COUNT(*) AS pixel_count
    FROM read_parquet('{final_parquet}')
    GROUP BY user_id_hashed
    ORDER BY pixel_count DESC
    LIMIT {limit};
    """
    
    df_users = duckdb.query(query).to_df()

    print(f"Retrieved {len(df_users)} users with the most pixels placed.")
    return df_users

def user_pixel_coords(final_parquet, top_user_ids):
    top_user_ids_str = ",".join(f"'{user_id}'" for user_id in top_user_ids)
    
    query = f"""
    SELECT x, y, user_id_hashed, pixel_color, timestamp
    FROM read_parquet('{final_parquet}')
    WHERE user_id_hashed IN ({top_user_ids_str})
    ORDER BY timestamp DESC
    """

    df_positions = duckdb.query(query).to_df()
    
    print(f"Retrieved {len(df_positions)} pixel positions for top users.")
    return df_positions

def regenerate_image(final_parquet, pixel_x, pixel_y, radius=50):
    query = f"""
    SELECT x, y, timestamp, pixel_color
    FROM read_parquet('{final_parquet}')
    WHERE x >= {pixel_x - radius} AND x <= {pixel_x + radius}
    AND y >= {pixel_y - radius} AND y <= {pixel_y + radius}
    AND timestamp >= '2022-04-01 00:00:00' AND timestamp < '2022-04-04 24:00:00'
    """
    
    pixels = duckdb.query(query).to_df()

    image_size = (2 * radius + 1, 2 * radius + 1)
    image = Image.new('RGB', image_size)

    for _, row in pixels.iterrows():
        x, y, hex_color = row['x'], row['y'], row['pixel_color']
        rgb_color = hex_to_rgb(hex_color)
        image.putpixel((x - (pixel_x - radius), y - (pixel_y - radius)), rgb_color)

    image.save(f"{pixel_x},{pixel_y}.png")
    image.show()

def regenerate_canvas(df_positions, canvas_size):
    image = Image.new('RGB', canvas_size, (255, 255, 255)) 

    for _, row in df_positions.iterrows():
        x, y, hex_color = row['x'], row['y'], row['pixel_color']
        rgb_color = hex_to_rgb(hex_color)
        image.putpixel((x, y), rgb_color)  

    image.show()
    image.save("most_active_users_canvas.png")

def main():
    final_parquet = "final.parquet"
    top_users_result = top_users(final_parquet, limit=10)
    top_user_ids = top_users_result['user_id_hashed'].tolist()
    coordinates_result = user_pixel_coords(final_parquet, top_user_ids)
    print(coordinates_result.head())
    regenerate_image(final_parquet, 929, 1858)
    regenerate_image(final_parquet, 1647, 244)
    regenerate_canvas(coordinates_result, canvas_size=(2000, 2000))  

if __name__ == "__main__":
    main()
    