# Chức năng hệ thống
- Khảo sát thông tin các video trong kênh
- Phân tích thống kê dựa trên các thông tin đó

# Mô hình hoạt động
![image](https://user-images.githubusercontent.com/84069686/184475741-78de76da-c46d-4d44-9de7-f55bd2493c91.png)

1. Youtube crawler: thu thập và làm sạch dữ liệu video trên kênh theo Airflow config
2. Kafka: stream dữ liệu đổ về
3. Spark Streaming: consum data vào lưu trữ data stream vào HDFS
4. HDFS: Lưu toàn bộ dữ liệu 
5. Spark: Xử lý dữ liệu để tối ưu cho phân tích
6. PowerBI: Phân tích dữ liệu
