import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType
from pyspark.sql.functions import col, count, hour, minute, second, sum as _sum
import pyspark.sql.functions as F

# Initialize SparkSession
spark = SparkSession.builder.appName("CustomerAnalytics").getOrCreate()

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

# Load the data
# Load the data
path = "/opt/spark/data/2019-Nov.csv"  # Adjust this to your actual file path
df = spark.read.format("csv") \
      .option("header", True) \
      .option('delimiter', ',') \
      .schema(schema) \
      .load(path)


# Check schema and preview data
df.printSchema()
df.show(5)

# 1. Distinct event types
df.select('event_type').distinct().show()

# 2. Group by category_id and event_type, aggregate counts
category_event_df = df.groupBy('category_id', 'event_type').agg(count('*').alias('counts'))

# 3. Pivot table on event_type for 'view' and 'cart', calculate cart-to-view ratio
category_pivot_df = category_event_df.groupBy("category_id") \
    .pivot("event_type", ["view", "cart"]) \
    .sum("counts") \
    .withColumn("cart2viewratio", (F.col('cart') / F.col('view'))) \
    .orderBy('cart2viewratio', ascending=False)

category_pivot_df.show()

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

brand_purchase_view_df.show()

# Retrieve the brand with the highest purchase-to-view ratio
top_brand = brand_purchase_view_df.first()
print(f"Top brand by purchase-to-view ratio: {top_brand[0]}")

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

# Show data for the brand 'pixiebelles'
brand_category_df.filter(col("brand") == 'pixiebelles') \
    .orderBy('purchase2viewratio', ascending=False).show()

# 8. Calculate revenue by category for purchases
revenue_df = df.groupBy('category_id', 'event_type') \
    .agg(_sum('price').alias('revenue')) \
    .filter(col('event_type') == 'purchase') \
    .orderBy('revenue', ascending=False)

revenue_df.show()

# Stop Spark session
spark.stop()
