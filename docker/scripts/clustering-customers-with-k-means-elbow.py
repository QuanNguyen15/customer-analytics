from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.sql.functions import col, count, mean, countDistinct
import os

# Initialize Spark session
spark = SparkSession.builder \
    .appName("CustomerAnalysis") \
    .getOrCreate()

# Define file paths
files = [
    "/opt/spark/data/2019-Nov.csv",
    "/opt/spark/data/2019-Dec.csv",
    "/opt/spark/data/2020-Jan.csv",
    "/opt/spark/data/2020-Feb.csv",
    "/opt/spark/data/2020-Mar.csv",
    "/opt/spark/data/2020-Apr.csv"
]

# Function to process each file
def process_file(file_path):
    # Extract month and year from file name
    file_name = os.path.basename(file_path)
    month_year = file_name.split('.')[0]

    # Read the CSV file
    df = spark.read.csv(file_path, header=True, inferSchema=True)

    # Drop unnecessary columns
    df = df.drop("event_time", "user_session")

    # Encode categorical variables
    indexer = StringIndexer(inputCol="event_type", outputCol="event_type_index")
    encoder = OneHotEncoder(inputCols=["event_type_index"], outputCols=["event_type_encoded"])

    # Apply encoding
    df = indexer.fit(df).transform(df)
    df = encoder.fit(df).transform(df)

    # Create dummy variables for category_code
    df = df.withColumn("category_encoded", col("category_code"))

    # Group data by user_id
    user_features = df.groupBy("user_id").agg(
        count("event_type").alias("activity_frequency"),
        countDistinct("product_id").alias("unique_products"),
        countDistinct("brand").alias("unique_brands"),
        mean("price").alias("average_spend")
    )

    # Prepare features for clustering
    feature_cols = ["activity_frequency", "unique_products", "unique_brands", "average_spend"]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    final_data = assembler.transform(user_features)

    # Choose the number of clusters
    optimal_k = 4

    # Perform K-means clustering
    kmeans = KMeans(k=optimal_k, seed=1)
    model = kmeans.fit(final_data)

    # Add cluster predictions to the dataset
    clustered_data = model.transform(final_data)

    # Analyze clusters
    cluster_analysis = clustered_data.groupBy("prediction").agg(
        count("user_id").alias("num_users"),
        mean("activity_frequency").alias("avg_activity"),
        mean("unique_products").alias("avg_unique_products"),
        mean("average_spend").alias("avg_spend"),
        mean("unique_brands").alias("avg_unique_brands")
    )

    # Convert Spark DataFrame to Pandas for easier manipulation
    pandas_cluster_analysis = cluster_analysis.toPandas()

    # Function to determine behavior and strategy
    def get_behavior_and_strategy(row):
        if row['avg_spend'] > 500 and row['avg_activity'] < 10:
            return ("High spenders with low activity", 
                    "Offer personalized high-end product suggestions")
        elif row['avg_spend'] <= 300 and row['avg_activity'] <= 10:
            return ("Price-sensitive and low engagement shoppers", 
                    "Encourage purchases with small discounts")
        elif row['avg_activity'] > 200 and row['avg_unique_products'] > 100:
            return ("Highly active explorers", 
                    "Use personalized recommendations and dynamic content")
        elif row['avg_activity'] > 50 and row['avg_spend'] > 200:
            return ("Moderately active, diverse brand interactions", 
                    "Encourage loyalty with brand-specific promotions")
        else:
            return ("Average activity and spend", 
                    "Maintain engagement with a balanced approach")

    # Apply behavior and strategy analysis
    pandas_cluster_analysis[['Behavior', 'Strategy']] = pandas_cluster_analysis.apply(get_behavior_and_strategy, axis=1, result_type='expand')

    # Save results to CSV
    output_file = f'/results/cluster_analysis_results_{month_year}.csv'
    pandas_cluster_analysis.to_csv(output_file, index=False)
    print(f"Analysis for {month_year} saved to {output_file}")

# Process each file
for file in files:
    process_file(file)

# Stop Spark session
spark.stop()