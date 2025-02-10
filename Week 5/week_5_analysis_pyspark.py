from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col, desc
import pandas as pd
from PIL import Image

spark = (
    SparkSession.builder
    .appName("r_place_analysis")
    .config("spark.driver.bindAddress", "127.0.0.1")  
    .config("spark.memory.offHeap.enabled", "true")
    .config("spark.memory.offHeap.size", "10g")
    .getOrCreate()
)

df = spark.read.parquet("final.parquet")

def hex_to_rgb(hex_color):
    hex_color = hex_color.lstrip('#')
    return tuple(int(hex_color[i:i+2], 16) for i in (0, 2, 4))

def top_users(df, limit=50):
    df_users = (
        df.groupBy("user_id_hashed")
        .agg(count("*").alias("pixel_count"))
        .orderBy(desc("pixel_count"))
        .limit(limit)
    )
    print(f"Retrieved {df_users.count()} users with the most pixels placed.")
    return df_users.toPandas()

def user_pixel_positions(df, top_user_ids):
    df_positions = (
        df.filter(col("user_id_hashed").isin(top_user_ids))
        .select("x", "y", "user_id_hashed", "pixel_color", "timestamp")
        .orderBy(desc("timestamp"))
    )
    print(f"Retrieved {df_positions.count()} pixel positions for top users.")
    return df_positions.toPandas()

def regenerate_image(pixel_x, pixel_y, radius=50):
    pixels = df.filter(
        (df.x >= pixel_x - radius) & (df.x <= pixel_x + radius) &
        (df.y >= pixel_y - radius) & (df.y <= pixel_y + radius)
    ).select('x', 'y', 'pixel_color')

    pixels_pd = pixels.toPandas()
    image_size = (2 * radius + 1, 2 * radius + 1)
    image = Image.new('RGB', image_size, (255, 255, 255))  

    for _, row in pixels_pd.iterrows():
        x, y, hex_color = row['x'], row['y'], row['pixel_color']
        rgb_color = hex_to_rgb(hex_color)


        image_x = x - (pixel_x - radius)
        image_y = y - (pixel_y - radius)

        image.putpixel((image_x, image_y), rgb_color)
       
    image.save(f"PYSPARK_{pixel_x},{pixel_y}.png")
    image.show()


def regenerate_canvas(df_positions, canvas_size):
    image = Image.new('RGB', canvas_size, (255, 255, 255)) 

    for _, row in df_positions.iterrows():
        x, y, hex_color = row['x'], row['y'], row['pixel_color']
        rgb_color = hex_to_rgb(hex_color)
        image.putpixel((x, y), rgb_color)  

    image.show()
    image.save("pyspark_most_active_users_canvas.png")

def main():
    top_users_result = top_users(df, limit=10)
    print(top_users_result.head())
    top_user_ids = top_users_result["user_id_hashed"].tolist()
    coordinates_result = user_pixel_positions(df, top_user_ids)
    print(coordinates_result.head())
    regenerate_image(929, 1858)
    # regenerate_image(1647, 244)
    regenerate_canvas(coordinates_result, canvas_size=(2000, 2000))  
    
if __name__ == "__main__":
    main()
