import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType
from pyspark.sql.functions import col, count, hour, minute, second, sum as _sum, input_file_name
import pyspark.sql.functions as F
import logging

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define the schema
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

# Read all files into a single DataFrame
df = spark.read.csv(file_paths, header=True, schema=schema)
df = df.withColumn("input_file", input_file_name())

# Perform analysis on the entire DataFrame
results = analyze_file(df)

# Display and save results
logger.info("Distinct event types across all files:")
results['event_types'].show()

logger.info("Top categories by cart-to-view ratio across all files:")
results['category_pivot'].orderBy('cart2viewratio', ascending=False).show()

logger.info("Top brands by purchase-to-view ratio across all files:")
results['brand_purchase_view'].orderBy('purchase2viewratio', ascending=False).show()

logger.info("Brand 'pixiebelles' data across all files:")
results['brand_category'].filter(col("brand") == 'pixiebelles').orderBy('purchase2viewratio', ascending=False).show()

logger.info("Top categories by revenue across all files:")
results['revenue'].orderBy('revenue', ascending=False).show()

# Save aggregated results to CSV
output_dir = "/opt/spark/data/output_all_files"
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

results['revenue'].write.mode("overwrite").csv(f"{output_dir}/aggregated_revenue")
logger.info(f"Aggregated results saved to {output_dir}")

# Stop Spark session
spark.stop()