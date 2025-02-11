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

def top_users(df, limit=50):
    df_users = (
        df.groupBy("user_id_hashed")
        .agg(count("*").alias("pixel_count"))
        .orderBy(desc("pixel_count"))
        .limit(limit)
    )
    return df_users.toPandas()

def user_pixel_positions(df, top_user_ids):
    df_positions = (
        df.filter(col("user_id_hashed").isin(top_user_ids))
        .select("x", "y", "user_id_hashed", "pixel_color", "timestamp")
        .orderBy(desc("timestamp"))
    )
    return df_positions.toPandas()

def main():
    top_users_result = top_users(df, limit=10)
    print(top_users_result.head())
    top_user_ids = top_users_result["user_id_hashed"].tolist()
    coordinates_result = user_pixel_positions(df, top_user_ids)
    print(coordinates_result.head())
    
if __name__ == "__main__":
    main()
