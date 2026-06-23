-- 创建topic
for topic in dwd_cart_add dwd_order_detail; do
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic $topic --partitions 3
done

-- 删除topic
for topic in dwd_cart_add dwd_order_detail; do
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic $topic
done