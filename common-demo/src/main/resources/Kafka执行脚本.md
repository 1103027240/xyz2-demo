-- 1、创建topic
-- DIM层
for topic in canal-topic; do
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic $topic --partitions 3
done

-- DWD层
for topic in dwd_cart_add dwd_order_detail; do
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic $topic --partitions 3
done

-- 2、删除topic
for topic in dwd_cart_add dwd_order_detail; do
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic $topic
done