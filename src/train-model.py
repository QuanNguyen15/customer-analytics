from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer, OneHotEncoder
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
import os

# Khởi tạo SparkSession
spark = SparkSession.builder.appName("Customer Segmentation").getOrCreate()

# Đọc dữ liệu từ 7 file CSV và gộp lại thành một DataFrame
file_paths = [
    "/opt/spark/data/2020-Jan.csv",
    "/opt/spark/data/2020-Feb.csv",
    "/opt/spark/data/2020-Mar.csv",
    "/opt/spark/data/2020-Apr.csv",
]

# Đọc và gộp dữ liệu
df_spark_list = [spark.read.csv(file, header=True, inferSchema=True) for file in file_paths]
df_spark = df_spark_list[0]
for df in df_spark_list[1:]:
    df_spark = df_spark.union(df)

# Xử lý các giá trị thiếu
mean_price = df_spark.select('price').na.drop().agg({'price': 'avg'}).collect()[0][0]
df_spark = df_spark.fillna({'price': mean_price})

# Loại bỏ các cột không cần thiết
df_spark = df_spark.drop('event_time', 'user_id', 'user_session', 'product_id')

# Xác định các cột phân loại
categorical_cols = ['category_code', 'brand']

# Xử lý các cột phân loại
for col_name in categorical_cols:
    num_unique_values = df_spark.select(col_name).distinct().count()
    if num_unique_values > 1:  # Chỉ mã hóa nếu có ít nhất hai giá trị khác nhau
        indexer = StringIndexer(inputCol=col_name, outputCol=col_name + '_index')
        df_spark = indexer.fit(df_spark).transform(df_spark)
        encoder = OneHotEncoder(inputCols=[col_name + '_index'], outputCols=[col_name + '_vec'])
        df_spark = encoder.fit(df_spark).transform(df_spark)
    else:
        print(f"Cột '{col_name}' không có đủ giá trị khác nhau để mã hóa. Bỏ qua cột này.")

# Chọn các cột đầu vào và chuẩn bị dữ liệu
input_cols = [col for col in df_spark.columns if col not in ['price']]
assembler = VectorAssembler(inputCols=input_cols, outputCol='features')
df_spark = assembler.transform(df_spark)

# Tiền xử lý dữ liệu
scaler = StandardScaler(inputCol='features', outputCol='scaled_features')
df_spark = scaler.fit(df_spark).transform(df_spark)

# Chọn cột đặc trưng
df_spark = df_spark.select(['scaled_features', 'price'])

# Chia dữ liệu thành tập huấn luyện và tập kiểm tra
train_data, test_data = df_spark.randomSplit([0.8, 0.2], seed=1234)

# Khởi tạo mô hình KMeans
kmeans = KMeans(k=3, seed=1234, featuresCol='scaled_features', predictionCol='cluster')

# Huấn luyện mô hình
kmeans_model = kmeans.fit(train_data)

# Dự đoán trên tập kiểm tra
predictions = kmeans_model.transform(test_data)

# Đánh giá mô hình
evaluator = ClusteringEvaluator(predictionCol="cluster")

# Silhouette Score
silhouette = evaluator.evaluate(predictions)
print(f'Silhouette Score: {silhouette}')

# Tính toán WSSSE (Within Set Sum of Squared Errors)
wssse = kmeans_model.computeCost(test_data)
print(f'Within Set Sum of Squared Errors (WSSSE): {wssse}')

# Lưu mô hình
model_path = "/tmp/customer_segmentation_model.mdl"
if not os.path.exists(model_path):
    os.makedirs(model_path)
kmeans_model.write().overwrite().save(model_path)
print(f"Mô hình đã được lưu tại: {model_path}")

# Lưu kết quả phân nhóm vào file CSV
result_path = "/tmp/output_with_clusters.csv"
predictions.write.csv(result_path, header=True)
print(f"Kết quả phân nhóm đã được lưu tại: {result_path}")
