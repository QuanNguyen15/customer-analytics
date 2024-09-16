from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, month, year, desc, rank, to_timestamp, broadcast
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType, DoubleType

def create_spark_session():
    return SparkSession.builder \
        .appName("OptimizedProductAnalysis") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.default.parallelism", "200") \
        .config("spark.sql.broadcastTimeout", "600") \
        .config("spark.executor.instances", "5") \
        .config("spark.executor.cores", "4") \
        .config("spark.executor.memory", "8g") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

def define_schema():
    return StructType([
        StructField("event_time", TimestampType(), True),
        StructField("event_type", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("category_id", LongType(), True),
        StructField("category_code", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("user_id", StringType(), True),
        StructField("user_session", StringType(), True)
    ])

def read_csv(spark, file_path, schema):
    return spark.read.csv(file_path, header=True, schema=schema)

def process_data(df):
    df_filtered = df.filter(col("event_type").isin(["view", "cart"]))

    # Tính toán interest_count và thêm cột month, year
    result = df_filtered.groupBy("product_id", "category_id", month("event_time").alias("month"), year("event_time").alias("year")) \
        .agg(count("*").alias("interest_count"))

    # Xếp hạng trong mỗi danh mục và tháng
    window = Window.partitionBy("category_id", "month", "year").orderBy(desc("interest_count"))
    return result.withColumn("rank", rank().over(window)).filter(col("rank") <= 10)

def main():
    spark = create_spark_session()
    schema = define_schema()
    
    # Đọc danh sách file
    import glob
    file_paths = glob.glob("/opt/spark/data/*.csv")
    
    # Đọc và xử lý tất cả các file
    all_data = spark.createDataFrame([], schema)
    for file_path in file_paths:
        df = read_csv(spark, file_path, schema)
        all_data = all_data.union(df)
    
    # Xử lý dữ liệu
    final_df = process_data(all_data)
    
    # Cache kết quả để tăng tốc các operation tiếp theo
    final_df.cache()
    
    # Hiển thị kết quả
    final_df.show()
    
    # Lưu kết quả
    final_df.write.partitionBy("year", "month").csv("/opt/spark/output/product_analysis_results", header=True, mode="overwrite")
    
    spark.stop()

if __name__ == "__main__":
    main()