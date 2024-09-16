# customer-analytics
Analyze customer data to gain insights, segment markets, and enhance marketing strategies. Uses machine learning algorithms to process large-scale data from CRM and e-commerce platforms. Built with Python, Hadoop/Spark for big data processing, and ML libraries for customer behavior prediction.


# Phân tích Dữ liệu Khách hàng và Xây dựng Chiến lược Tiếp thị

## Mục tiêu chính:
- Hiểu rõ hành vi khách hàng, phân khúc thị trường, và tối ưu hóa chiến lược tiếp thị dựa trên phân tích dữ liệu.
- Cung cấp các thông tin chi tiết về sản phẩm, hành vi mua hàng, và xu hướng tiêu dùng để hỗ trợ ra quyết định kinh doanh.

## Yêu cầu và Hướng dẫn Thực hiện:

### 1. Thu thập và xử lý dữ liệu khách hàng
   - **Nguồn dữ liệu**: Thu thập dữ liệu từ các hệ thống CRM, nền tảng thương mại điện tử, hoặc từ các file CSV chứa thông tin lịch sử mua sắm của khách hàng.
   - **Công cụ xử lý**: Sử dụng Hadoop, Spark, hoặc Hive để xử lý dữ liệu lớn và thực hiện phân tích dữ liệu hiệu quả.
   - **Mục tiêu**: Chuẩn bị dữ liệu cho các phân tích chuyên sâu về hành vi và phân khúc khách hàng.

### 2. Phân tích hành vi khách hàng thông qua biểu đồ trực quan
   Sử dụng các công cụ như Python (matplotlib, seaborn) hoặc Power BI để trực quan hóa dữ liệu và đưa ra các thông tin chi tiết quan trọng về hành vi mua sắm và sở thích của khách hàng.

   #### 2.1 Phân tích sản phẩm được quan tâm nhưng chưa mua  
   - **Mục tiêu**: Xác định các sản phẩm mà khách hàng xem nhiều nhưng chưa mua (event_type = “view”).
   - **Chiến lược**: Sử dụng kết quả này để đề xuất các chiến lược marketing nhằm thúc đẩy quyết định mua hàng cho những sản phẩm đang được quan tâm.
   - **Biểu đồ**: Plot biểu đồ dạng cột hoặc biểu đồ đường để thấy sự tương tác của khách hàng với từng sản phẩm.

   #### 2.2 Phân tích sản phẩm ưa chuộng và ít được quan tâm  
   - **Mục tiêu**: Xác định loại sản phẩm nào được mua nhiều (event_type = “purchase”) và loại nào ít được quan tâm (event_type = “view” nhiều nhưng không mua).
   - **Biểu đồ**: Plot biểu đồ phân phối (distribution plot) để so sánh tỷ lệ mua và xem của các loại sản phẩm.
   - **Chiến lược**: Đề xuất các chương trình khuyến mãi hoặc quảng cáo cho những sản phẩm ít được mua, đồng thời tối ưu hóa việc quảng bá các sản phẩm được ưa chuộng.

   #### 2.3 Phân tích nhãn hàng được khách hàng ưa chuộng  
   - **Mục tiêu**: Xác định các nhãn hàng trong cùng một loại sản phẩm mà khách hàng yêu thích.
   - **Biểu đồ**: Plot biểu đồ so sánh tỷ lệ mua hàng giữa các nhãn hiệu cùng loại (brand-wise comparison).
   - **Chiến lược**: Đề xuất hợp tác với các nhãn hàng nổi bật, hoặc cải thiện quảng cáo cho các nhãn ít được ưa chuộng.

   #### 2.4 Phân tích hành vi khách hàng theo thời gian (giữa các tháng)  
   - **Mục tiêu**: So sánh hành vi khách hàng giữa các tháng để xác định sự thay đổi trong tần suất mua hàng, sản phẩm yêu thích, số tiền chi tiêu trung bình, và các hành vi khác.
   - **Biểu đồ**: Plot biểu đồ so sánh theo thời gian để thấy xu hướng tiêu dùng qua từng tháng.
   - **Chiến lược**: Điều chỉnh chiến dịch kinh doanh dựa trên sự thay đổi theo mùa hoặc các dịp khuyến mãi đặc biệt.

### 3. Thực hiện phân khúc thị trường  
   - **Phương pháp**: Sử dụng các thuật toán phân cụm (clustering) như K-Means hoặc DBSCAN để phân chia khách hàng thành các nhóm dựa trên hành vi mua sắm và tương tác của họ.
   - **Dữ liệu đầu vào**: Các yếu tố cần phân tích bao gồm loại sản phẩm khách hàng mua, tần suất mua hàng, giá trị đơn hàng trung bình, thời gian mua gần nhất.
   - **Mục tiêu**: Xác định các nhóm khách hàng có đặc điểm chung (ví dụ: khách hàng trung thành, khách hàng mới, khách hàng tiềm năng) để có chiến lược tiếp thị phù hợp cho từng phân khúc.

### 4. Phân tích xu hướng tiêu dùng  
   - **Mục tiêu**: Hiểu rõ động lực, sở thích, và nhu cầu của khách hàng khi mua sản phẩm.
   - **Phân tích**: Xác định các yếu tố tác động đến quyết định mua hàng của khách hàng (ví dụ: giá cả, chất lượng sản phẩm, thương hiệu, khuyến mãi).
   - **Chiến lược**: Sử dụng thông tin này để cải thiện chiến lược giá cả, điều chỉnh sản phẩm hoặc tăng cường khuyến mãi nhằm thúc đẩy mua hàng.

### 5. Xây dựng mô hình dự đoán hành vi khách hàng  
   - **Mục tiêu**: Dự đoán hành vi trong tương lai của khách hàng, giúp doanh nghiệp điều chỉnh chiến lược kịp thời.
   - **Phương pháp**: Sử dụng các mô hình học máy như hồi quy logistic, mô hình cây quyết định (Decision Tree), hoặc mạng nơ-ron nhân tạo (Neural Networks) để dự đoán các kịch bản sau:
     - **Xác suất rời bỏ khách hàng** (churn prediction): Xác định nhóm khách hàng có nguy cơ rời bỏ dịch vụ.
     - **Khả năng mua lại**: Dự đoán khách hàng sẽ quay lại mua sản phẩm nào trong tương lai.
     - **Sản phẩm tiềm năng**: Dự đoán sản phẩm mà khách hàng có khả năng sẽ quan tâm dựa trên lịch sử mua hàng.
   - **Dữ liệu đầu vào**: Dữ liệu về hành vi mua hàng trong các tháng trước, loại sản phẩm, tần suất mua sắm, và các sự kiện khác liên quan đến tương tác của khách hàng.
   - **Chiến lược**: Điều chỉnh chiến lược kinh doanh và tiếp thị dựa trên sự thay đổi trong hành vi khách hàng theo thời gian.

### 6. Đề xuất các chiến lược tiếp thị  
   - **Phân khúc khách hàng**: Tùy chỉnh chiến lược tiếp thị cho từng nhóm khách hàng dựa trên phân tích phân khúc và hành vi mua sắm của họ.
   - **Chiến lược quảng cáo**: Tạo nội dung quảng cáo và chiến dịch tiếp thị phù hợp với sở thích và nhu cầu của từng nhóm khách hàng.
   - **Thúc đẩy tiêu dùng**: Sử dụng các chiến dịch tiếp thị cá nhân hóa dựa trên hành vi của khách hàng để tăng tỷ lệ chuyển đổi và mức độ trung thành.

## Kết luận:
Bài toán phân tích dữ liệu khách hàng yêu cầu việc thu thập, phân tích hành vi, và dự đoán xu hướng tiêu dùng của khách hàng. Từ đó, đưa ra các chiến lược tiếp thị và cải thiện trải nghiệm khách hàng nhằm tăng doanh số và độ trung thành của khách hàng.





# Docker Setup Guide
This guide will walk you through setting up a Docker environment, copying data into a Docker volume, starting containers, and cleaning up Docker resources such as containers, images, volumes, and networks.
## 1. Create Docker Volume
First, create a Docker volume to store data:
```bash
docker volume create spark-data
```
## 2. Copy Data from Local Machine to the Volume
Run the following command to copy data from your local machine into the created Docker volume (`spark-data`):
```bash
docker run --rm -v spark-data:/data -v /d/Big_data/customer-analytics/data:/source alpine sh -c "cp -R /source/* /data/"
```
In this command:  
- `spark-data:/data` mounts the `spark-data` volume to the container's `/data` directory.  
- `/d/Big_data/customer-analytics/data:/source` mounts the local directory `/d/Big_data/customer-analytics/data` to the container's `/source` directory.  
- The `cp` command copies all files from `/source/` to `/data/`.
## 3. Start the Spark Container
Start your container using Docker Compose:
```bash
docker-compose up -d
```
The `-d` flag runs the containers in detached mode.
## 4. Clean Up Docker Images and Containers
### 4.1. Check Running Containers
Before stopping or removing containers, check which ones are running:
```bash
docker ps
```
### 4.2. Stop Running Containers
Stop the running containers by using their `containerID`:
```bash
docker stop <containerID>
```
Replace `<containerID>` with the actual container ID from the previous step.
### 4.3. Remove Containers
Once the containers are stopped, remove them:
```bash
docker rm <containerID>
```
### 4.4. Remove Docker Images
Remove specific images by their `imageID`:
```bash
docker rmi <imageID> --force
```
The `--force` flag ensures that images are removed even if they are being used by stopped containers.
### 4.5. Remove Unused Images
Remove all unused images, including dangling ones:
```bash
docker image prune -a
```
This command will remove all unused images, not just dangling ones.
## 5. Remove Docker Volumes
To remove volumes, use the following command with the `volumeName`:
```bash
docker volume rm <volumeName>
```
Replace `<volumeName>` with the name of the volume you want to remove.
## 6. Remove Docker Networks
To remove networks, use the following command with the `networkName`:
```bash
docker network rm <networkName>
```
Replace `<networkName>` with the name of the network you want to remove.
## 7. Verify the Cleanup
After cleanup, you can verify that the images have been removed by running:
```bash
docker images
```
This command lists all remaining Docker images on your system.
---
You can now follow these instructions to manage your Docker environment effectively.
```