import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, hour, dayofmonth
import pandas as pd
import logging

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize SparkSession
spark = SparkSession.builder.appName("Discount Analysis").getOrCreate()

# Define file paths
files = [
    "/opt/spark/data/2019-Nov.csv",
    "/opt/spark/data/2019-Dec.csv",
    "/opt/spark/data/2020-Jan.csv",
    "/opt/spark/data/2020-Feb.csv",
    "/opt/spark/data/2020-Mar.csv",
    "/opt/spark/data/2020-Apr.csv"
]

# Define the output folder
output_folder = '/tmp/discount-time'

# Define function to analyze and save data
def analyze_and_save(file_path, output_folder):
    # Read data
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    file_name = os.path.basename(file_path).split('.')[0]  # Get file name without extension

    logger.info(f"Processing file: {file_name}")

    # Filter for discount-related events
    df = df.filter(col('event_type') == 'discount_purchase')

    # Convert 'event_time' to datetime format
    df = df.withColumn(
        'event_time', 
        date_format(col('event_time').substr(1, 19), 'yyyy-MM-dd HH:mm:ss')
    )
    
    # Extract hour and day from 'event_time'
    df = df.withColumn('hour', hour(col('event_time')))
    df = df.withColumn('day', dayofmonth(col('event_time')))

    # Hourly discount analysis
    hour_counts = df.groupBy('hour').count().orderBy('hour')

    # Daily discount analysis
    day_counts = df.groupBy('day').count().orderBy('day')

    # Convert Spark DataFrames to Pandas DataFrames
    hour_counts_pd = hour_counts.toPandas()
    day_counts_pd = day_counts.toPandas()
    
    # Ensure the output folder exists
    os.makedirs(output_folder, exist_ok=True)
    
    # Define the Excel file output path
    output_excel_path = f'{output_folder}/{file_name}-discount-analysis.xlsx'
    
    # Save results to an Excel file with multiple sheets
    with pd.ExcelWriter(output_excel_path, engine='openpyxl') as writer:
        hour_counts_pd.to_excel(writer, sheet_name='Hourly Discounts', index=False)
        day_counts_pd.to_excel(writer, sheet_name='Daily Discounts', index=False)

    logger.info(f"Analysis for {file_name} saved to {output_excel_path}")


# Process each file
for file_path in files:
    analyze_and_save(file_path, output_folder)

# Stop the Spark session
spark.stop()

logger.info("All analyses completed and Spark session stopped.")
