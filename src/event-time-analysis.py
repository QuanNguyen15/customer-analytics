import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType
from pyspark.sql.functions import col, count, hour, minute, second, input_file_name
import pyspark.sql.functions as F
import pandas as pd
import logging

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define the schema for the input CSV files
schema = StructType() \
    .add("event_time", TimestampType(), True) \
    .add("event_type", StringType(), True) \
    .add("product_id", StringType(), True) \
    .add("category_id", StringType(), True) \
    .add("category_code", StringType(), True) \
    .add("brand", StringType(), True) \
    .add("price", DoubleType(), True) \
    .add("user_id", StringType(), True) \
    .add("user_session", StringType(), True)

# Function to perform analysis on a DataFrame
def analyze_file(df):
    # 1. Group by category_code, brand, and event_type, aggregate counts
    category_event_df = df.groupBy('category_code', 'brand', 'event_type') \
                          .agg(count('*').alias('counts'))

    # 2. Pivot table to get counts for each event_type (view, cart, purchase, remove_from_cart)
    category_pivot_df = category_event_df.groupBy('category_code', 'brand') \
        .pivot('event_type', ['view', 'cart', 'purchase', 'remove_from_cart']) \
        .sum('counts') \
        .na.fill(0)  # Fill null values with 0

    # 3. Calculate ratios (if needed for visualization)
    category_pivot_df = category_pivot_df.withColumn("cart_to_view_ratio", F.col('cart') / F.col('view')) \
                                         .withColumn("purchase_to_view_ratio", F.col('purchase') / F.col('view')) \
                                         .withColumn("remove_from_cart_to_cart_ratio", F.col('remove_from_cart') / F.col('cart'))

    return category_pivot_df

# Initialize SparkSession on the driver node
spark = SparkSession.builder \
    .appName("CategoryCodeAnalysis") \
    .getOrCreate()

# List of file paths to process
file_paths = [
    "/opt/spark/data/2019-Oct.csv",
    "/opt/spark/data/2019-Nov.csv",
    "/opt/spark/data/2019-Dec.csv",
    "/opt/spark/data/2020-Jan.csv",
    "/opt/spark/data/2020-Feb.csv",
    "/opt/spark/data/2020-Mar.csv",
    "/opt/spark/data/2020-Apr.csv",
]

# Directory to store the output CSV files
output_dir = "/tmp/ratio-category-code-output"
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# Function to process each file and save the results
def process_and_save_file(file_path):
    # Read the data from the CSV file
    df = spark.read.csv(file_path, header=True, schema=schema)

    # Perform analysis on the file
    result_df = analyze_file(df)

    # Extract the month from the file path
    month_name = os.path.basename(file_path).split('-')[1].split('.')[0]

    # Define the CSV file path
    csv_file_path = f"{output_dir}/ratio_category_analysis_{month_name}.csv"

    # Convert Spark DataFrame to Pandas DataFrame for saving to CSV
    result_pandas_df = result_df.toPandas()

    # Save the results into CSV file
    result_pandas_df.to_csv(csv_file_path, index=False)

    logger.info(f"Results saved for {month_name} in {csv_file_path}")

# Process each file individually
for file_path in file_paths:
    process_and_save_file(file_path)

# Stop Spark session
spark.stop()
