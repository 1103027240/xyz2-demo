-- DWS层
-- 独立用户加购表
CREATE TABLE IF NOT EXISTS dws.cart_statistics (
    dt DATE,
    window_start DATETIME,
    window_end DATETIME,
    cart_add_cnt BIGINT
)
ENGINE = OLAP
PRIMARY KEY(dt, window_start, window_end)
PARTITION BY RANGE(dt) ()
DISTRIBUTED BY HASH(window_start) BUCKETS 8
PROPERTIES (
    "replication_num" = "3",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-7",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "8"
);

-- 交易域SKU-订单统计表
CREATE TABLE IF NOT EXISTS dws.trade_sku_order_statistics (
    dt DATE,
    window_start DATETIME,
    window_end DATETIME,
    trade_mark_id VARCHAR,
    trade_mark_name VARCHAR,
    category1_id VARCHAR,
    category1_name VARCHAR,
    category2_id VARCHAR,
    category2_name VARCHAR,
    category3_id VARCHAR,
    category3_name VARCHAR,
    sku_id VARCHAR,
    sku_name VARCHAR,
    spu_id VARCHAR,
    spu_name VARCHAR,
    original_amount DECIMAL(16, 2),
    activity_reduce_amount DECIMAL(16, 2),
    coupon_reduce_amount DECIMAL(16, 2),
    order_amount DECIMAL(16, 2)
)
ENGINE = OLAP
PRIMARY KEY(dt, window_start, window_end, sku_id)
PARTITION BY RANGE(dt) ()
DISTRIBUTED BY HASH(sku_id) BUCKETS 8
PROPERTIES (
    "replication_num" = "3",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-7",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "8"
);
