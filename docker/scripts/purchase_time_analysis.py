from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, dayofmonth
import pandas as pd

# Initialize Spark session
spark = SparkSession.builder \
    .appName("PurchaseTimeAnalysis") \
    .getOrCreate()

# Define a function to convert 'event_time' and filter 'purchase' events
def parse_event_time_and_filter(row):
    from datetime import datetime
    
    try:
        # Parse 'event_time' as timestamp (remove UTC if needed)
        event_time = datetime.strptime(row['event_time'][:19], '%Y-%m-%d %H:%M:%S')
        row['event_time'] = event_time
        
        # Filter 'purchase' events
        if row['event_type'] == 'purchase':
            return row
        else:
            return None
    except:
        return None

# List of file paths to process
file_paths = [
    "/opt/spark/data/2020-Feb.csv",
    "/opt/spark/data/2020-Mar.csv",
    "/opt/spark/data/2020-Apr.csv"
]

# Create an RDD for all the files
rdd = spark.sparkContext.emptyRDD()

# Read the data from each file and append to the RDD
for file_path in file_paths:
    csv_rdd = spark.read.csv(file_path, header=True).rdd
    rdd = rdd.union(csv_rdd)

# Apply transformations on the RDD to filter and extract necessary data
filtered_rdd = rdd.map(parse_event_time_and_filter).filter(lambda x: x is not None)

# Map RDD to extract hours and days from 'event_time'
hour_rdd = filtered_rdd.map(lambda row: (row['event_time'].hour, 1))  # (hour, 1)
day_rdd = filtered_rdd.map(lambda row: (row['event_time'].day, 1))  # (day, 1)

# Reduce by key to count occurrences of each hour and day
hour_counts_rdd = hour_rdd.reduceByKey(lambda a, b: a + b).sortByKey()
day_counts_rdd = day_rdd.reduceByKey(lambda a, b: a + b).sortByKey()

# Collect results to driver for further analysis
hour_counts_collected = hour_counts_rdd.collect()
day_counts_collected = day_counts_rdd.collect()

# Convert to pandas DataFrame for easier manipulation
hour_counts_df = pd.DataFrame(hour_counts_collected, columns=['hour', 'count'])
day_counts_df = pd.DataFrame(day_counts_collected, columns=['day', 'count'])

# Get the hour that appears most often and its count
most_common_hour = hour_counts_df.loc[hour_counts_df['count'].idxmax()]
print(f"Most common hour for purchases: {most_common_hour['hour']} with {most_common_hour['count']} purchases.")

# Get the top 3 hours with the most purchases
top_3_hours = hour_counts_df.nlargest(3, 'count')
print(f"Top 3 hours with most purchases:\n{top_3_hours}")

# Get the day that appears most often and its count
most_common_day = day_counts_df.loc[day_counts_df['count'].idxmax()]
print(f"Most common day for purchases: {most_common_day['day']} with {most_common_day['count']} purchases.")

# Get the top 3 days with the most purchases
top_3_days = day_counts_df.nlargest(3, 'count')
print(f"Top 3 days with most purchases:\n{top_3_days}")

# Stop the Spark session
spark.stop()
