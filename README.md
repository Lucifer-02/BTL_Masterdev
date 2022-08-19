# Chức năng hệ thống
- Khảo sát thông tin các video trong kênh
- Phân tích thống kê dựa trên các thông tin đó

# Mô hình hoạt động
![image](https://user-images.githubusercontent.com/84069686/185624149-bf30ba0e-de5c-49bc-8dba-562a873d3e59.png)

1. Youtube crawler: tự động thu thập và làm sạch dữ liệu video trên kênh 
2. Kafka: stream dữ liệu đổ về
3. Spark Streaming: consum data vào lưu trữ data stream vào HDFS
4. HDFS: Lưu toàn bộ dữ liệu 
5. Spark: Xử lý dữ liệu để tối ưu cho phân tích
6. Superset: Phân tích và visualize dữ liệu qua Hive
