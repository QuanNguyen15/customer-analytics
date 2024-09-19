import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType
from pyspark.sql.functions import col, count, sum as _sum
import pyspark.sql.functions as F
import pandas as pd
import logging

# Khởi tạo logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Định nghĩa schema cho các file CSV đầu vào
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

# Hàm phân tích dữ liệu trong DataFrame
def analyze_file(df):
    # 1. Tạo DataFrame với số lượng mua hàng và tổng chi tiêu của từng người dùng
    user_metrics_df = df.filter(col("event_type") == "purchase") \
        .groupBy("user_id") \
        .agg(
            count("*").alias("purchase_count"),
            _sum("price").alias("total_spend")
        )

    # 2. Phân khúc người dùng dựa trên tổng chi tiêu của họ
    spend_segments_df = user_metrics_df.withColumn(
        "spend_segment",
        F.when(col("total_spend") >= 1000, "Chi tiêu cao")
        .when((col("total_spend") >= 500) & (col("total_spend") < 1000), "Chi tiêu trung bình")
        .otherwise("Chi tiêu thấp")
    )

    # 3. Tính toán chi tiêu trung bình và số lần mua hàng trung bình theo từng phân khúc
    segment_summary_df = spend_segments_df.groupBy("spend_segment") \
        .agg(
            F.mean("total_spend").alias("avg_spend"),
            F.mean("purchase_count").alias("avg_purchase_count")
        ).orderBy("spend_segment")

    # 4. Tính toán tần suất mua hàng và chi tiêu theo thương hiệu
    brand_spend_df = df.filter(col("event_type") == "purchase") \
        .groupBy("brand") \
        .agg(
            count("*").alias("purchase_count"),
            _sum("price").alias("total_revenue")
        ).orderBy("total_revenue", ascending=False)

    return {
        'spend_segments': spend_segments_df,
        'segment_summary': segment_summary_df,
        'brand_spend': brand_spend_df
    }

# Khởi tạo SparkSession
spark = SparkSession.builder \
    .appName("CustomerSegmentationAnalysis") \
    .getOrCreate()

# Danh sách các file để xử lý
file_paths = [
    # "/opt/spark/data/2019-Oct.csv",
    # "/opt/spark/data/2019-Nov.csv",
    # "/opt/spark/data/2019-Dec.csv",
    "/opt/spark/data/2020-Jan.csv",
    "/opt/spark/data/2020-Feb.csv",
    "/opt/spark/data/2020-Mar.csv",
    "/opt/spark/data/2020-Apr.csv",
]

# Thư mục để lưu các file Excel đầu ra
output_dir = "/tmp/customer-segmentation-output"
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# Hàm xử lý và lưu kết quả cho từng file
def process_and_save_file(file_path):
    # Đọc dữ liệu từ file CSV
    df = spark.read.csv(file_path, header=True, schema=schema)

    # Thực hiện phân tích trên dữ liệu
    results = analyze_file(df)

    # Lấy tháng từ đường dẫn file
    month_name = os.path.basename(file_path).split('-')[1].split('.')[0]

    # Định nghĩa đường dẫn file Excel
    excel_file_path = f"{output_dir}/customer_segmentation_{month_name}.xlsx"

    # Chuyển đổi các DataFrame Spark thành DataFrame Pandas để lưu vào Excel
    spend_segments_df = results['spend_segments'].toPandas()
    segment_summary_df = results['segment_summary'].toPandas()
    brand_spend_df = results['brand_spend'].toPandas()

    # Lưu kết quả vào các sheet khác nhau trong cùng một file Excel
    with pd.ExcelWriter(excel_file_path, engine='openpyxl') as writer:
        spend_segments_df.to_excel(writer, sheet_name='Phân Khúc Khách Hàng', index=False)
        segment_summary_df.to_excel(writer, sheet_name='Tóm Tắt Phân Khúc', index=False)
        brand_spend_df.to_excel(writer, sheet_name='Chi Tiêu Theo Thương Hiệu', index=False)

    logger.info(f"Kết quả đã được lưu cho {month_name} tại {excel_file_path}")

# Xử lý từng file riêng biệt
for file_path in file_paths:
    process_and_save_file(file_path)

# Dừng Spark session
spark.stop()
