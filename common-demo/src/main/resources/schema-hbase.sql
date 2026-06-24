-- 1、Phoenix（sqlline.py）
-- 建库
CREATE SCHEMA IF NOT EXISTS "XYZ2_DEMO";

-- 建视图：常用视图
-- SKU信息
CREATE VIEW "XYZ2_DEMO"."dim_sku_info" (
    "id" VARCHAR PRIMARY KEY,
    "info"."spu_id" VARCHAR,
    "info"."price" VARCHAR,
    "info"."sku_name" VARCHAR,
    "info"."sku_desc" VARCHAR,
    "info"."weight" VARCHAR,
    "info"."tm_id" VARCHAR,
    "info"."category3_id" VARCHAR,
    "info"."sku_default_img" VARCHAR,
    "info"."is_sale" VARCHAR,
    "info"."create_time" VARCHAR
)
COLUMN_ENCODED_BYTES = 0;

-- SPU信息
CREATE VIEW "XYZ2_DEMO"."dim_spu_info" (
    "id" VARCHAR PRIMARY KEY,
    "info"."spu_name" VARCHAR,
    "info"."description" VARCHAR,
    "info"."category3_id" VARCHAR,
    "info"."tm_id" VARCHAR
)
COLUMN_ENCODED_BYTES = 0;

-- 品牌信息
CREATE VIEW "XYZ2_DEMO"."dim_base_trademark" (
    "id" VARCHAR PRIMARY KEY,
    "info"."tm_name" VARCHAR
)
COLUMN_ENCODED_BYTES = 0;

-- 分类3
CREATE VIEW "XYZ2_DEMO"."dim_base_category3" (
    "id" VARCHAR PRIMARY KEY,
    "info"."name" VARCHAR,
    "info"."category2_id" VARCHAR
)
COLUMN_ENCODED_BYTES = 0;

-- 分类2
CREATE VIEW "XYZ2_DEMO"."dim_base_category2" (
    "id" VARCHAR PRIMARY KEY,
    "info"."name" VARCHAR,
    "info"."category1_id" VARCHAR
)
COLUMN_ENCODED_BYTES = 0;

-- 分类1
CREATE VIEW "XYZ2_DEMO"."dim_base_category1" (
    "id" VARCHAR PRIMARY KEY,
    "info"."name" VARCHAR
)
COLUMN_ENCODED_BYTES = 0;

-- 省份信息
CREATE VIEW "XYZ2_DEMO"."dim_base_province" (
    "id" VARCHAR PRIMARY KEY,
    "info"."name" VARCHAR,
    "info"."region_id" VARCHAR,
    "info"."area_code" VARCHAR,
    "info"."iso_code" VARCHAR,
    "info"."iso_3166_2" VARCHAR
)
COLUMN_ENCODED_BYTES = 0;



