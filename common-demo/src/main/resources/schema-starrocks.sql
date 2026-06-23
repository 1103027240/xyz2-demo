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
