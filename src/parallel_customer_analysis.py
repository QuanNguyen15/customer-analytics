import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType
from pyspark.sql.functions import col, count, hour, minute, second, sum as _sum, input_file_name
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
    # 1. Distinct event types
    event_types = df.select('event_type').distinct()

    # 2. Group by category_id and event_type, aggregate counts
    category_event_df = df.groupBy('category_id', 'event_type').agg(count('*').alias('counts'))

    # 3. Pivot table on event_type for 'view' and 'cart', calculate cart-to-view ratio
    category_pivot_df = category_event_df.groupBy("category_id") \
        .pivot("event_type", ["view", "cart"]) \
        .sum("counts") \
        .withColumn("cart2viewratio", (F.col('cart') / F.col('view'))) \
        .orderBy('cart2viewratio', ascending=False)

    # 4. Group by event_time, event_type, and brand, aggregate counts
    time_event_brand_df = df.groupBy('event_time', 'event_type', 'brand') \
        .agg(count('*').alias('counts'))

    # 5. Filter for events between 12 AM to 6 AM
    filtered_df = time_event_brand_df.filter(
        ((hour(col("event_time")) == 6) & (minute(col("event_time")) == 0) & (second(col("event_time")) == 0)) |
        (hour(col("event_time")) <= 5)
    )

    # 6. Pivot by event_type for 'view' and 'purchase', calculate purchase-to-view ratio
    brand_purchase_view_df = filtered_df.groupBy("brand") \
        .pivot("event_type", ["view", "purchase"]) \
        .sum("counts") \
        .withColumn("purchase2viewratio", (F.col('purchase') / F.col('view'))) \
        .orderBy('purchase2viewratio', ascending=False)

    # 7. Filter and group by brand and category, calculate purchase-to-view ratio
    filtered_brand_cat_df = df.groupBy('event_time', 'event_type', 'brand', 'category_id') \
        .agg(count('*').alias('counts')) \
        .filter(
            ((hour(col("event_time")) == 6) & (minute(col("event_time")) == 0) & (second(col("event_time")) == 0)) |
            (hour(col("event_time")) <= 5)
        )

    brand_category_df = filtered_brand_cat_df.groupBy("brand", 'category_id') \
        .pivot("event_type", ["view", "purchase"]) \
        .sum("counts") \
        .withColumn("purchase2viewratio", (F.col('purchase') / F.col('view')))

    # 8. Calculate revenue by category for purchases
    revenue_df = df.groupBy('category_id', 'event_type') \
        .agg(_sum('price').alias('revenue')) \
        .filter(col('event_type') == 'purchase') \
        .orderBy('revenue', ascending=False)

    return {
        'event_types': event_types,
        'category_pivot': category_pivot_df,
        'brand_purchase_view': brand_purchase_view_df,
        'brand_category': brand_category_df,
        'revenue': revenue_df
    }

# Initialize SparkSession on the driver node
spark = SparkSession.builder \
    .appName("ParallelCustomerAnalysis") \
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

# Directory to store the output Excel files
output_dir = "/tmp/customer-behaviour-output"
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# Function to process each file and save the results
def process_and_save_file(file_path):
    # Read the data from the CSV file
    df = spark.read.csv(file_path, header=True, schema=schema)

    # Perform analysis on the file
    results = analyze_file(df)

    # Extract the month from the file path
    month_name = os.path.basename(file_path).split('-')[1].split('.')[0]

    # Define the Excel file path
    excel_file_path = f"{output_dir}/purchase_analysis_{month_name}.xlsx"

    # Convert Spark DataFrames to Pandas DataFrames for saving to Excel
    event_types_df = results['event_types'].toPandas()
    category_pivot_df = results['category_pivot'].toPandas()
    brand_purchase_view_df = results['brand_purchase_view'].toPandas()
    brand_category_df = results['brand_category'].toPandas()
    revenue_df = results['revenue'].toPandas()

    # Save the results into different sheets of the same Excel file
    with pd.ExcelWriter(excel_file_path, engine='openpyxl') as writer:
        event_types_df.to_excel(writer, sheet_name='Event Types', index=False)
        category_pivot_df.to_excel(writer, sheet_name='Cart-to-View Ratio', index=False)
        brand_purchase_view_df.to_excel(writer, sheet_name='Purchase-to-View Ratio', index=False)
        brand_category_df.to_excel(writer, sheet_name='Brand-Category Analysis', index=False)
        revenue_df.to_excel(writer, sheet_name='Revenue by Category', index=False)

    logger.info(f"Results saved for {month_name} in {excel_file_path}")

# Process each file individually
for file_path in file_paths:
    process_and_save_file(file_path)

# Stop Spark session
spark.stop()
