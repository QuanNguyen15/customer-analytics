from pyspark.sql import SparkSession

# Tạo Spark session
spark = SparkSession.builder.appName("CustomerPurchaseAnalysis").getOrCreate()

# Đọc dữ liệu từ file CSV
data = spark.read.csv("/opt/spark/data/2020-Jan.csv", header=True, inferSchema=True)

# Xử lý dữ liệu (giả sử đã thực hiện các bước xử lý dữ liệu ở trước đó)
# Ví dụ, nếu cần lọc chỉ những sự kiện mua sắm
purchase_events = data.filter(data['event_type'] == 'purchase')

# Nhóm theo ID khách hàng và đếm số lượng giao dịch
user_purchases = purchase_events.groupBy("user_id").count()

# Hiển thị kết quả
user_purchases.show(50)  # Hiển thị 50 dòng dữ liệu
