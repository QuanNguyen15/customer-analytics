from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, dayofmonth

# Initialize Spark session
spark = SparkSession.builder \
    .appName("PurchaseTimeAnalysis") \
    .getOrCreate()

# Load the data for October
df_oct = spark.read.csv('/opt/spark/data/2019-Oct.csv', header=True, inferSchema=True)

# Convert 'event_time' to timestamp and remove 'UTC'
df_oct = df_oct.withColumn('event_time', df_oct['event_time'].substr(1, 19).cast('timestamp'))

# Filter for 'purchase' event_type
purchase_df_oct = df_oct.filter(df_oct['event_type'] == 'purchase')

# Extract hour from 'event_time'
purchase_df_oct = purchase_df_oct.withColumn('hour', hour(purchase_df_oct['event_time']))

# Count occurrences of each hour
hour_counts = purchase_df_oct.groupBy('hour').count().orderBy('hour')

# Collect results to driver
hour_counts_collected = hour_counts.collect()

# Convert to pandas DataFrame for easier manipulation
import pandas as pd
hour_counts_df = pd.DataFrame(hour_counts_collected, columns=['hour', 'count'])

# Get the hour that appears most often and its count
most_common_hour = hour_counts_df.loc[hour_counts_df['count'].idxmax()]
print(f"Most common hour for purchases: {most_common_hour['hour']} with {most_common_hour['count']} purchases.")

# Get the top 3 hours with the most purchases
top_3_hours = hour_counts_df.nlargest(3, 'count')
print(f"Top 3 hours with most purchases:\n{top_3_hours}")

# Extract day from 'event_time'
purchase_df_oct = purchase_df_oct.withColumn('day', dayofmonth(purchase_df_oct['event_time']))

# Count occurrences of each day
day_counts = purchase_df_oct.groupBy('day').count().orderBy('day')

# Collect results to driver
day_counts_collected = day_counts.collect()

# Convert to pandas DataFrame for easier manipulation
day_counts_df = pd.DataFrame(day_counts_collected, columns=['day', 'count'])

# Get the day that appears most often and its count
most_common_day = day_counts_df.loc[day_counts_df['count'].idxmax()]
print(f"Most common day for purchases: {most_common_day['day']} with {most_common_day['count']} purchases.")

# Get the top 3 days with the most purchases
top_3_days = day_counts_df.nlargest(3, 'count')
print(f"Top 3 days with most purchases:\n{top_3_days}")

# Stop the Spark session
spark.stop()
