-- DWS层
-- 独立用户加购表
CREATE TABLE IF NOT EXISTS dws.cart_statistics (
    dt DATE,
    window_start DATETIME,
    window_end DATETIME,
    cart_add_cnt BIGINT REPLACE
)
ENGINE = OLAP
AGGREGATE KEY(dt, window_start, window_end)
PARTITION BY RANGE(dt) ()
DISTRIBUTED BY HASH(window_start) BUCKETS 10
PROPERTIES (
    "replication_num" = "3",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "10"
);

-- 交易域SKU-订单统计表
CREATE TABLE IF NOT EXISTS dws.trade_sku_order_statistics (
    dt DATE,
    window_start DATETIME,
    window_end DATETIME,
    trade_mark_id VARCHAR(32),
    trade_mark_name VARCHAR(100),
    category1_id VARCHAR(32),
    category1_name VARCHAR(200),
    category2_id VARCHAR(32),
    category2_name VARCHAR(200),
    category3_id VARCHAR(32),
    category3_name VARCHAR(200),
    sku_id VARCHAR(32),
    sku_name VARCHAR(200),
    spu_id VARCHAR(32),
    spu_name VARCHAR(200),
    original_amount DECIMAL(16, 2) REPLACE,
    activity_reduce_amount DECIMAL(16, 2) REPLACE,
    coupon_reduce_amount DECIMAL(16, 2) REPLACE,
    order_amount DECIMAL(16, 2) REPLACE
)
ENGINE = OLAP
AGGREGATE KEY(dt, window_start, window_end, trade_mark_id, trade_mark_name, category1_id, category1_name, category2_id, category2_name,
category3_id, category3_name, sku_id, sku_name, spu_id, spu_name)
PARTITION BY RANGE(dt) ()
DISTRIBUTED BY HASH(window_start) BUCKETS 10
PROPERTIES (
    "replication_num" = "3",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "10"
);

-- 交易域省份-订单统计表
CREATE TABLE IF NOT EXISTS dws.trade_province_order_statistics (
    dt DATE,
    window_start DATETIME,
    window_end DATETIME,
    province_id VARCHAR(32),
    province_name VARCHAR(64),
    order_count BIGINT REPLACE,
    order_amount DECIMAL(16, 2) REPLACE
)
ENGINE = OLAP
    AGGREGATE KEY(dt, window_start, window_end, province_id, province_name)
PARTITION BY RANGE(dt) ()
DISTRIBUTED BY HASH(window_start) BUCKETS 10
PROPERTIES (
    "replication_num" = "3",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "10"
);
