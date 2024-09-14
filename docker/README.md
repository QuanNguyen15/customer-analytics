# Hướng Dẫn Cấu Hình Docker Volume và Cài Đặt Hive Cho Dự Án Phân Tích Dữ Liệu Khách Hàng

Bài hướng dẫn này sẽ giúp bạn cài đặt và cấu hình Docker cùng Apache Hive để thực hiện phân tích dữ liệu khách hàng. Chúng ta sẽ sử dụng Docker Volume để lưu trữ dữ liệu một cách an toàn, đảm bảo dữ liệu không bị mất khi container Hive dừng. 

## 1. Cấu hình Docker Volume để lưu trữ dữ liệu
Docker Volume giúp lưu trữ dữ liệu ngoài container, đảm bảo rằng dữ liệu không bị mất khi container bị dừng hoặc xóa.

### Bước 1: Tạo Docker Volume

Tạo Docker volume để lưu trữ dữ liệu cho Hive:

```bash
docker volume create hive-data
```

Volume này sẽ được sử dụng để lưu dữ liệu bên trong container Hive.

### Bước 2: Sao chép dữ liệu từ máy local vào Docker Volume

Docker không hỗ trợ sao chép dữ liệu trực tiếp vào volume, vì vậy bạn cần sử dụng một container tạm thời. Đầu tiên, khởi chạy một container `busybox` để gắn volume và sao chép dữ liệu:

```bash
docker run -v hive-data:/opt/hive/data --name helper-container -it busybox
```

Sử dụng lệnh `docker cp` để sao chép dữ liệu từ máy local vào volume:

```bash
docker cp D:/Big_data/customer-analytics/data/. helper-container:/opt/hive/data/
```

### Bước 3: Kiểm tra dữ liệu đã sao chép

Kiểm tra xem dữ liệu đã được sao chép thành công vào Docker volume chưa:

```bash
docker exec -it helper-container ls /opt/hive/data
```

Nếu dữ liệu hiển thị chính xác, bạn có thể xóa container tạm thời:

```bash
docker rm -f helper-container
```

### Bước 4: Chạy container Hive và gắn Docker Volume

Bây giờ bạn có thể chạy container Hive và gắn volume `hive-data` để đảm bảo dữ liệu được lưu trữ ổn định khi container dừng hoặc khởi động lại.

```bash
docker run -d -v hive-data:/opt/hive/data --name hive4 apache/hive:4.0.0
```

### Bước 5: Kiểm tra dữ liệu trong container Hive

Để xác nhận dữ liệu đã được gắn thành công vào container Hive, bạn có thể kiểm tra nội dung của thư mục `/opt/hive/data` bên trong container:

```bash
docker exec -it hive4 bash
ls /opt/hive/data
```

Dữ liệu sẽ hiển thị nếu việc sao chép thành công.

### Bước 6: Dữ liệu vẫn tồn tại sau khi container dừng

Nếu bạn dừng hoặc khởi động lại container, dữ liệu trong Docker volume vẫn sẽ được giữ nguyên:

```bash
docker stop hive4
docker rm hive4
```

Khởi động lại container và xác nhận dữ liệu vẫn còn:

```bash
docker run -d --platform linux/amd64 -p 10000:10000 -p 10002:10002 -v hive-data:/opt/hive/data --env SERVICE_NAME=hiveserver2 --name hive4 apache/hive:4.0.0
```

## 2. Cài đặt và cấu hình Hive

Tiếp theo, bạn sẽ cài đặt và cấu hình Hive trên container để thực hiện phân tích dữ liệu khách hàng.

### Bước 1: Khởi tạo container Hive

Sử dụng lệnh sau để khởi tạo container Hive:

```bash
docker run -d --platform linux/amd64 -p 10000:10000 -p 10002:10002 --env SERVICE_NAME=hiveserver2 --name hive4 apache/hive:4.0.0
```

Lệnh này sẽ chạy container Hive với HiveServer2 được bật.

### Bước 2: Sao chép file dữ liệu vào container Hive

Sao chép file CSV chứa dữ liệu phân tích vào container Hive:

```bash
docker cp data/processed/2019-Oct.csv hive4:/mnt
```

### Bước 3: Truy cập vào Hive thông qua Beeline

Sử dụng Beeline để truy cập vào Hive bên trong container:

```bash
docker exec -it hive4 beeline -u 'jdbc:hive2://localhost:10000/'
```

### Bước 4: Tạo cơ sở dữ liệu trong Hive

Tạo một cơ sở dữ liệu để lưu trữ dữ liệu phân tích:

```sql
CREATE DATABASE IF NOT EXISTS retail_db;
USE retail_db;
```

### Bước 5: Tạo bảng Hive để lưu trữ dữ liệu phân tích

Tạo bảng Hive để lưu dữ liệu từ file CSV:

```sql
CREATE TABLE IF NOT EXISTS ecommerce_events(
  event_time STRING,
  event_type STRING,
  product_id STRING,
  category_id STRING,
  category_code STRING,
  brand STRING,
  price STRING,
  user_id STRING,
  user_session STRING
) ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE;
```

### Bước 6: Nạp dữ liệu từ file CSV vào bảng

Sử dụng lệnh sau để nạp dữ liệu từ file CSV vào bảng `ecommerce_events`:

```sql
LOAD DATA LOCAL INPATH '/mnt/2019-Oct.csv' INTO TABLE ecommerce_events;
```

Hoặc nạp dữ liệu từ volume Docker đã tạo:

```sql
LOAD DATA LOCAL INPATH '/opt/hive/data/2020-Apr.csv' INTO TABLE ecommerce_events;
```

### Bước 7: Quản lý kết nối và thuộc tính kết nối Hive

Bạn có thể sử dụng lệnh sau để kết nối với cơ sở dữ liệu Hive khác nếu cần:

```sql
!connect jdbc:hive2://localhost:10000/db-name
```

## 3. Kết luận

Qua bài hướng dẫn này, bạn đã biết cách:

- Cấu hình Docker Volume để lưu trữ dữ liệu an toàn cho container Hive.
- Sao chép dữ liệu từ máy local vào Docker Volume.
- Cài đặt và cấu hình Hive trên Docker để thực hiện phân tích dữ liệu.

Sử dụng Docker Volume giúp bạn giữ dữ liệu an toàn và không bị mất khi container bị dừng hoặc xóa.