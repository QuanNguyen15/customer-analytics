from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, dayofmonth
import pandas as pd
import os

# Initialize Spark session
spark = SparkSession.builder \
    .appName("PurchaseTimeAnalysis") \
    .getOrCreate()

# List of file paths to process (dữ liệu cho các tháng khác nhau)
file_paths = [
    "/opt/spark/data/2019-Oct.csv",
    "/opt/spark/data/2019-Nov.csv",
    "/opt/spark/data/2019-Dec.csv",
    "/opt/spark/data/2020-Jan.csv",
    "/opt/spark/data/2020-Feb.csv",
    "/opt/spark/data/2020-Mar.csv",
    "/opt/spark/data/2020-Apr.csv"
]

# Directory to store the output Excel files (tạo thư mục output nếu chưa có)
output_dir = "/tmp/purchase_analysis_output"
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# Function to process each file and save to Excel with 2 sheets
def process_file(file_path):
    # Read the data from the CSV file
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    
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

    # Extract the month name from file path (for naming the output file)
    month_name = os.path.basename(file_path).split('-')[1].split('.')[0]

    # Define the Excel file path
    excel_file_path = f"{output_dir}/purchase_stats_{month_name}.xlsx"

    # Save to Excel with two sheets: one for hourly counts and one for daily counts
    with pd.ExcelWriter(excel_file_path, engine='openpyxl') as writer:
        hour_counts_df.to_excel(writer, sheet_name='Hourly Counts', index=False)
        day_counts_df.to_excel(writer, sheet_name='Daily Counts', index=False)

    # Print to console the month processed
    print(f"Data for {month_name} saved: {excel_file_path}")

# Process each file
for file_path in file_paths:
    process_file(file_path)

# Stop the Spark session
spark.stop()
