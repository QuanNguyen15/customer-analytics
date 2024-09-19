import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType
from pyspark.sql.functions import col, count, countDistinct, sum as _sum, collect_set, concat_ws, when
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
def analyze_data(df):
    # 1. Tạo DataFrame với thông tin mua hàng của khách hàng
    user_purchase_df = df.filter(col("event_type") == "purchase") \
        .groupBy("user_id") \
        .agg(
            countDistinct("category_code").alias("distinct_categories"),
            collect_set("category_code").alias("categories"),
            countDistinct("brand").alias("distinct_brands"),
            count("*").alias("order_count"),
            _sum("price").alias("total_spend"),
            collect_set("brand").alias("brands")
        )

    # 2. Xác định khách hàng trung thành (mua ít nhất 2 đơn hàng, nhiều nhãn hàng và danh mục khác nhau)
    loyal_customers_df = user_purchase_df.filter(
        (col("order_count") >= 2) & (col("distinct_brands") > 1) & (col("distinct_categories") > 1)
    ).select("user_id")

    # 3. Xác định khách hàng tiềm năng (mua ít nhất 2 đơn hàng, nhưng chưa đạt đủ tiêu chí trung thành)
    potential_customers_df = user_purchase_df.filter(
        (col("order_count") >= 2) & ((col("distinct_brands") == 1) | (col("distinct_categories") == 1))
    ).select("user_id")

    # 4. Tạo DataFrame với phân khúc khách hàng và thông tin mua hàng
    all_customers_df = user_purchase_df.join(
        loyal_customers_df,
        on="user_id",
        how="left"
    ).join(
        potential_customers_df,
        on="user_id",
        how="left"
    )

    # 5. Xác định phân khúc khách hàng
    customer_segments_df = all_customers_df.withColumn(
        "customer_type",
        F.when(col("user_id").isNotNull(), "Loyal")
         .when(col("user_id").isNotNull(), "Potential")
         .otherwise("Non-Loyal")
    ).select(
        "user_id", 
        "customer_type", 
        "total_spend", 
        "categories", 
        "brands"
    )

    # 6. Xử lý giá trị null cho cột mảng và cột số
    customer_segments_df = customer_segments_df.withColumn(
        "categories",
        when(col("categories").isNull(), F.array()).otherwise(col("categories"))
    ).withColumn(
        "brands",
        when(col("brands").isNull(), F.array()).otherwise(col("brands"))
    ).fillna({
        "total_spend": 0.0
    })

    # 7. Chuyển đổi các cột mảng thành chuỗi
    customer_segments_df = customer_segments_df.withColumn(
        "categories", concat_ws(", ", "categories")
    ).withColumn(
        "brands", concat_ws(", ", "brands")
    )

    # 8. Tính toán số lượng khách hàng theo phân khúc
    segment_summary_df = customer_segments_df.groupBy("customer_type") \
        .agg(count("*").alias("customer_count"))

    return {
        'customer_segments': customer_segments_df,
        'segment_summary': segment_summary_df
    }

# Khởi tạo SparkSession
spark = SparkSession.builder \
    .appName("CustomerSegmentationAnalysis") \
    .getOrCreate()

# Danh sách các file để xử lý
file_paths = [
    "/opt/spark/data/2020-Jan.csv",
    "/opt/spark/data/2020-Feb.csv",
    "/opt/spark/data/2020-Mar.csv",
    "/opt/spark/data/2020-Apr.csv",
]

# Thư mục để lưu các file Excel đầu ra
output_dir = "/tmp/customer-segmentation-output"
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# Đọc tất cả các tệp CSV vào một DataFrame duy nhất
df_all = spark.read.csv(file_paths, header=True, schema=schema)

# Tạo một DataFrame duy nhất cho mỗi tháng
for file_path in file_paths:
    # Lấy tháng từ đường dẫn file
    month_name = os.path.basename(file_path).split('-')[1].split('.')[0]
    
    # Lọc dữ liệu cho tháng cụ thể
    df_month = df_all.filter(F.date_format(col("event_time"), "yyyy-MM") == f"2020-{month_name}")
    
    # Thực hiện phân tích trên dữ liệu tháng
    results = analyze_data(df_month)

    # Định nghĩa đường dẫn file Excel
    excel_file_path = f"{output_dir}/customer_segmentation_{month_name}.xlsx"

    # Chuyển đổi các DataFrame Spark thành DataFrame Pandas để lưu vào Excel
    customer_segments_df = results['customer_segments'].toPandas()
    segment_summary_df = results['segment_summary'].toPandas()

    # Lưu kết quả vào các sheet khác nhau trong cùng một file Excel
    with pd.ExcelWriter(excel_file_path, engine='openpyxl') as writer:
        customer_segments_df.to_excel(writer, sheet_name='Customer Segments', index=False)
        segment_summary_df.to_excel(writer, sheet_name='Segment Summary', index=False)

    logger.info(f"Kết quả đã được lưu cho {month_name} tại {excel_file_path}")

# Dừng Spark session
spark.stop()
