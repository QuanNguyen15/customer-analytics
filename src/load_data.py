from pyspark.sql import SparkSession

# Tạo Spark session
spark = SparkSession.builder.appName("CustomerPurchaseAnalysis").getOrCreate()

# Đọc dữ liệu từ file CSV
data = spark.read.csv("/opt/spark/data/2020-Jan.csv", header=True, inferSchema=True)

purchase_events = data.filter(data['event_type'] == 'purchase')

user_purchases = purchase_events.groupBy("user_id").count()

user_purchases.show(50)
