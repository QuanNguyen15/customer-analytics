import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, hour, dayofmonth
import pandas as pd
import logging

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize SparkSession
spark = SparkSession.builder.appName("Ecommerce Analysis").getOrCreate()

# Define function to analyze and save data
def analyze_and_save(file_path, output_folder):
    # Read data
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    file_name = os.path.basename(file_path).split('.')[0]  # Get file name without extension

    logger.info(f"Processing file: {file_name}")

    # 1. Analyze category distribution
    category_count = df.groupBy('category_code').count().orderBy('count', ascending=False)
    top_10_cate = category_count.limit(10)
    
    # 2. Analyze price changes
    price_changes = df.groupBy('product_id').agg({'price': 'count'}).filter(col('count(price)') > 1)
    products_with_price_change = price_changes.select('product_id').rdd.flatMap(lambda x: x).collect()
    df.filter(col('product_id').isin(products_with_price_change))
    
    # 3. Find minimum prices and associated records
    min_prices = df.groupBy('product_id').agg({'price': 'min'}).withColumnRenamed('min(price)', 'min_price')
    records_with_min_price = df.join(min_prices, 
                                     (df['product_id'] == min_prices['product_id']) & 
                                     (df['price'] == min_prices['min_price']))

    # Convert 'event_time' to datetime format
    records_with_min_price = records_with_min_price.withColumn(
        'event_time', 
        date_format(col('event_time').substr(1, 19), 'yyyy-MM-dd HH:mm:ss')
    )
    
    # 4. Hourly discount analysis
    records_with_min_price = records_with_min_price.withColumn('hour', hour(col('event_time')))
    hour_counts = records_with_min_price.groupBy('hour').count().orderBy('hour')
    
    # 5. Daily discount analysis
    records_with_min_price = records_with_min_price.withColumn('day', dayofmonth(col('event_time')))
    day_counts = records_with_min_price.groupBy('day').count().orderBy('day')

    # Convert Spark DataFrames to Pandas DataFrames
    top_10_cate_pd = top_10_cate.toPandas()
    hour_counts_pd = hour_counts.toPandas()
    day_counts_pd = day_counts.toPandas()
    
    # Ensure the output folder exists
    os.makedirs(output_folder, exist_ok=True)
    
    # Define the Excel file output path
    output_excel_path = f'{output_folder}/{file_name}-analysis.xlsx'
    
    # Save results to an Excel file with multiple sheets
    with pd.ExcelWriter(output_excel_path, engine='openpyxl') as writer:
        top_10_cate_pd.to_excel(writer, sheet_name='Top 10 Categories', index=False)
        hour_counts_pd.to_excel(writer, sheet_name='Hourly Discounts', index=False)
        day_counts_pd.to_excel(writer, sheet_name='Daily Discounts', index=False)

    logger.info(f"Analysis for {file_name} saved to {output_excel_path}")

# Define file paths
files = [
    # "/opt/spark/data/2019-Oct.csv",~
    "/opt/spark/data/2019-Nov.csv",
    "/opt/spark/data/2019-Dec.csv",
    "/opt/spark/data/2020-Jan.csv",
    "/opt/spark/data/2020-Feb.csv",
    "/opt/spark/data/2020-Mar.csv",
    "/opt/spark/data/2020-Apr.csv"
]

# Define the output folder
output_folder = '/tmp/discount-time'

# Process each file
for file_path in files:
    analyze_and_save(file_path, output_folder)

# Stop the Spark session
spark.stop()

logger.info("All analyses completed and Spark session stopped.")
