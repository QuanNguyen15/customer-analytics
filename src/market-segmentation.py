from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Create a Spark session
spark = SparkSession.builder \
    .appName("Ecommerce Analysis") \
    .getOrCreate()

# Define file paths
file_paths = [
    "/opt/spark/data/2019-Oct.csv",
    "/opt/spark/data/2019-Nov.csv",
    "/opt/spark/data/2019-Dec.csv"
]

# Function to process each file and save CSV output
def process_monthly_data(file_path, month_name, output_path):
    # Load the data into a Spark DataFrame
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    
    # Calculate the max price for each category
    window_category = Window.partitionBy("category_code")
    df = df.withColumn('max_price', F.max('price').over(window_category))
    
    # Define price group thresholds
    df = df.withColumn('group1_price', 0.20 * F.col('max_price'))
    df = df.withColumn('group2_price', 0.40 * F.col('max_price'))
    df = df.withColumn('group3_price', 0.60 * F.col('max_price'))
    df = df.withColumn('group4_price', 0.80 * F.col('max_price'))
    
    # Assign price group based on price
    df = df.withColumn(
        'price_group', 
        F.when(F.col('price') <= F.col('group1_price'), 1)
         .when(F.col('price') <= F.col('group2_price'), 2)
         .when(F.col('price') <= F.col('group3_price'), 3)
         .when(F.col('price') <= F.col('group4_price'), 4)
         .otherwise(5)
    )
    
    # Drop unnecessary columns
    df = df.drop('max_price', 'group1_price', 'group2_price', 'group3_price', 'group4_price')
    
    # Count the number of products bought by each customer in each price group
    customer_segment = df.groupby('user_id', 'price_group').count()
    
    # Pivot the data to have one row per user with their product count per price group
    customer_segment_pivot = customer_segment.groupBy("user_id").pivot("price_group").agg(F.first("count")).fillna(0)
    
    # Assign each customer to their preferred group (the group they bought the most from)
    group_cols = [str(i) for i in range(1, 6)]
    customer_segment_pivot = customer_segment_pivot.withColumn(
        'preferred_group',
        F.greatest(*[F.col(c).alias(c) for c in group_cols]).alias('preferred_group')
    )
    
    # Save the full data as CSV
    customer_segment_pivot.coalesce(1).write.csv(output_path, header=True, mode='overwrite')
    
    print(f"Data for {month_name} processed and saved to {output_path}")

# Process each month's data and save to CSV files
for file_path in file_paths:
    # Extract month name from file path
    month_name = file_path.split('/')[-1].split('.')[0]
    output_path = f"/tmp/market-segment-output/{month_name}.csv"
    
    # Process data and save to CSV
    process_monthly_data(file_path, month_name, output_path)

spark.stop()
