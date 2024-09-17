# # from pyspark.sql import SparkSession
# # from pyspark.sql.functions import hour, dayofmonth
# # import pandas as pd
# # import os

# # # Initialize Spark session
# # spark = SparkSession.builder \
# #     .appName("PurchaseTimeAnalysis") \
# #     .getOrCreate()

# # # Define a function to convert 'event_time' and filter 'purchase' events
# # def parse_event_time_and_filter(row):
# #     from datetime import datetime
    
# #     try:
# #         # Parse 'event_time' as timestamp (remove UTC if needed)
# #         event_time = datetime.strptime(row['event_time'][:19], '%Y-%m-%d %H:%M:%S')
# #         row['event_time'] = event_time
        
# #         # Filter 'purchase' events
# #         if row['event_type'] == 'purchase':
# #             return row
# #         else:
# #             return None
# #     except:
# #         return None

# # # List of file paths to process
# # file_paths = [
# #     "/opt/spark/data/2020-Apr.csv"
# # ]

# # # Create an RDD for all the files
# # rdd = spark.sparkContext.emptyRDD()

# # # Read the data from each file and append to the RDD
# # for file_path in file_paths:
# #     csv_rdd = spark.read.csv(file_path, header=True).rdd
# #     rdd = rdd.union(csv_rdd)

# # # Apply transformations on the RDD to filter and extract necessary data
# # filtered_rdd = rdd.map(parse_event_time_and_filter).filter(lambda x: x is not None)

# # # Map RDD to extract hours and days from 'event_time'
# # hour_rdd = filtered_rdd.map(lambda row: (row['event_time'].hour, 1))  # (hour, 1)
# # day_rdd = filtered_rdd.map(lambda row: (row['event_time'].day, 1))  # (day, 1)

# # # Reduce by key to count occurrences of each hour and day
# # hour_counts_rdd = hour_rdd.reduceByKey(lambda a, b: a + b).sortByKey()
# # day_counts_rdd = day_rdd.reduceByKey(lambda a, b: a + b).sortByKey()

# # # Collect results to driver for further analysis
# # hour_counts_collected = hour_counts_rdd.collect()
# # day_counts_collected = day_counts_rdd.collect()

# # # Convert to pandas DataFrame for easier manipulation
# # hour_counts_df = pd.DataFrame(hour_counts_collected, columns=['hour', 'count'])
# # day_counts_df = pd.DataFrame(day_counts_collected, columns=['day', 'count'])

# # # Specify output directory (can change it from /tmp to any other location)
# # output_dir = "/tmp/purchase_analysis_output"
# # if not os.path.exists(output_dir):
# #     os.makedirs(output_dir)

# # # Save the hour counts to a CSV file
# # hour_counts_df.to_csv(os.path.join(output_dir, 'hourly_purchase_counts.csv'), index=False)
# # print(f"Hourly purchase counts saved to {output_dir}/hourly_purchase_counts.csv")

# # # Save the day counts to a CSV file
# # day_counts_df.to_csv(os.path.join(output_dir, 'daily_purchase_counts.csv'), index=False)
# # print(f"Daily purchase counts saved to {output_dir}/daily_purchase_counts.csv")

# # # Stop the Spark session
# # spark.stop()


# from pyspark.sql import SparkSession
# from pyspark.sql.functions import hour, dayofmonth

# # Initialize Spark session
# spark = SparkSession.builder \
#     .appName("PurchaseTimeAnalysis") \
#     .getOrCreate()

# # Load the data for October
# df_oct = spark.read.csv('/opt/spark/data/2019-Oct.csv', header=True, inferSchema=True)

# # Convert 'event_time' to timestamp and remove 'UTC'
# df_oct = df_oct.withColumn('event_time', df_oct['event_time'].substr(1, 19).cast('timestamp'))

# # Filter for 'purchase' event_type
# purchase_df_oct = df_oct.filter(df_oct['event_type'] == 'purchase')

# # Extract hour from 'event_time'
# purchase_df_oct = purchase_df_oct.withColumn('hour', hour(purchase_df_oct['event_time']))

# # Count occurrences of each hour
# hour_counts = purchase_df_oct.groupBy('hour').count().orderBy('hour')

# # Collect results to driver
# hour_counts_collected = hour_counts.collect()

# # Convert to pandas DataFrame for easier manipulation
# import pandas as pd
# hour_counts_df = pd.DataFrame(hour_counts_collected, columns=['hour', 'count'])

# # Get the hour that appears most often and its count
# most_common_hour = hour_counts_df.loc[hour_counts_df['count'].idxmax()]
# print(f"Most common hour for purchases: {most_common_hour['hour']} with {most_common_hour['count']} purchases.")

# # Get the top 3 hours with the most purchases
# top_3_hours = hour_counts_df.nlargest(3, 'count')
# print(f"Top 3 hours with most purchases:\n{top_3_hours}")

# # Extract day from 'event_time'
# purchase_df_oct = purchase_df_oct.withColumn('day', dayofmonth(purchase_df_oct['event_time']))

# # Count occurrences of each day
# day_counts = purchase_df_oct.groupBy('day').count().orderBy('day')

# # Collect results to driver
# day_counts_collected = day_counts.collect()

# # Convert to pandas DataFrame for easier manipulation
# day_counts_df = pd.DataFrame(day_counts_collected, columns=['day', 'count'])

# # Get the day that appears most often and its count
# most_common_day = day_counts_df.loc[day_counts_df['count'].idxmax()]
# print(f"Most common day for purchases: {most_common_day['day']} with {most_common_day['count']} purchases.")

# # Get the top 3 days with the most purchases
# top_3_days = day_counts_df.nlargest(3, 'count')
# print(f"Top 3 days with most purchases:\n{top_3_days}")

# # Stop the Spark session
# spark.stop()




from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, dayofmonth
import pandas as pd

# Initialize Spark session
spark = SparkSession.builder \
    .appName("PurchaseTimeAnalysis") \
    .getOrCreate()

# List of file paths to process
file_paths = [
    "/opt/spark/data/2019-Oct.csv",
    # "/opt/spark/data/2019-Nov.csv",
    # "/opt/spark/data/2019-Dec.csv",
    # "/opt/spark/data/2020-Jan.csv",
    # "/opt/spark/data/2020-Feb.csv",
    # "/opt/spark/data/2020-Mar.csv",
    # "/opt/spark/data/2020-Apr.csv"
]

# Read and combine data from all files
df = spark.read.csv(file_paths, header=True, inferSchema=True)

# Convert 'event_time' to timestamp and remove 'UTC'
df = df.withColumn('event_time', df['event_time'].substr(1, 19).cast('timestamp'))

# Filter for 'purchase' event_type
purchase_df = df.filter(df['event_type'] == 'purchase')

# Extract hour and day from 'event_time'
purchase_df = purchase_df.withColumn('hour', hour(purchase_df['event_time']))
purchase_df = purchase_df.withColumn('day', dayofmonth(purchase_df['event_time']))

# Count occurrences of each hour
hour_counts = purchase_df.groupBy('hour').count().orderBy('hour')

# Count occurrences of each day
day_counts = purchase_df.groupBy('day').count().orderBy('day')

# Convert to pandas DataFrame for easier manipulation
hour_counts_df = hour_counts.toPandas()
day_counts_df = day_counts.toPandas()

# Save results to CSV files
hour_counts_df.to_csv('/tmp/hour-counts-2019-Oct.csv', index=False)
day_counts_df.to_csv('/tmp/day-counts-2019-Oct.csv', index=False)

# Get the hour that appears most often and its count
most_common_hour = hour_counts_df.loc[hour_counts_df['count'].idxmax()]

# Get the top 3 hours with the most purchases
top_3_hours = hour_counts_df.nlargest(3, 'count')

# Get the day that appears most often and its count
most_common_day = day_counts_df.loc[day_counts_df['count'].idxmax()]

# Get the top 3 days with the most purchases
top_3_days = day_counts_df.nlargest(3, 'count')

# Print results to console
print(f"Most common hour for purchases: {most_common_hour['hour']} with {most_common_hour['count']} purchases.")
print(f"Top 3 hours with most purchases:\n{top_3_hours}")

print(f"Most common day for purchases: {most_common_day['day']} with {most_common_day['count']} purchases.")
print(f"Top 3 days with most purchases:\n{top_3_days}")

# Stop the Spark session
spark.stop()

