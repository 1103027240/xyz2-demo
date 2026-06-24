-- 1、模拟加购
-- 1）创建
DELIMITER //

CREATE PROCEDURE InsertCartWithDelay()
BEGIN
    -- 声明变量
    DECLARE done INT DEFAULT FALSE;
    DECLARE v_user_id VARCHAR(200);
    DECLARE v_sku_id BIGINT;
    DECLARE v_sku_name VARCHAR(200);
    DECLARE v_sku_img VARCHAR(300);
    DECLARE v_cart_price DECIMAL(10, 2);
    DECLARE v_sku_num INT;
    DECLARE v_random_count INT;
    DECLARE v_i INT DEFAULT 0;
    DECLARE v_sleep_time INT;

    -- 声明游标
    DECLARE user_cur CURSOR FOR SELECT id FROM sync_test.user_info;
    -- 异常处理：如果没找到数据，继续执行，同时将done设为TRUE
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    OPEN user_cur;

    user_loop: LOOP
        FETCH user_cur INTO v_user_id;

        IF done THEN
            LEAVE user_loop;
        END IF;

        -- 每个用户随机加购 1~5 个 SKU
        SET v_random_count = FLOOR(1 + RAND() * 5);
        SET v_i = 0;

        WHILE v_i < v_random_count DO
            -- 随机取一个 SKU
            SELECT id, sku_name, sku_default_img, price
            INTO v_sku_id, v_sku_name, v_sku_img, v_cart_price
            FROM sync_test.sku_info
            ORDER BY RAND()
            LIMIT 1;

            -- 随机数量 1~10
            SET v_sku_num = FLOOR(1 + RAND() * 10);

            INSERT INTO sync_test.cart_info
            (user_id, sku_id, cart_price, sku_num, img_url, sku_name, is_checked, create_time, operate_time)
            VALUES
            (v_user_id,
             v_sku_id,
           v_cart_price,
           v_sku_num,
           v_sku_img,
             v_sku_name,
           1,
           NOW(),
           NOW());

            SET v_i = v_i + 1;
        END WHILE;

        -- 随机睡眠 3~5 秒
        SET v_sleep_time = FLOOR(3 + RAND() * 3);
        DO SLEEP(v_sleep_time);

    END LOOP;

    CLOSE user_cur;
END //
DELIMITER ;

-- 执行
CALL InsertCartWithDelay();

-- 2）执行
CALL InsertCartWithDelay();

-- 3）删除
DROP PROCEDURE IF EXISTS InsertCartWithDelay;

-- 4）停止
SHOW PROCESSLIST;
KILL <进程ID>;

-- 2、模拟下单
-- 1）创建
DELIMITER //

CREATE PROCEDURE InsertOrderWithDelay()
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE v_user_id BIGINT;
    DECLARE v_province_id INT;
    DECLARE v_sku_id BIGINT;
    DECLARE v_sku_name VARCHAR(200);
    DECLARE v_sku_img VARCHAR(300);
    DECLARE v_sku_price DECIMAL(10, 0);
    DECLARE v_sku_num BIGINT;
    DECLARE v_order_id BIGINT;
    DECLARE v_detail_id BIGINT;
    DECLARE v_detail_count INT;
    DECLARE v_i INT DEFAULT 0;
    DECLARE v_j INT DEFAULT 0;
    DECLARE v_sleep_time INT;
    DECLARE v_total_amount DECIMAL(16, 2);
    DECLARE v_original_total DECIMAL(16, 2);
    DECLARE v_activity_amount DECIMAL(16, 2);
    DECLARE v_coupon_amount DECIMAL(16, 2);
    DECLARE v_split_total DECIMAL(16, 2);
    DECLARE v_split_activity DECIMAL(16, 2);
    DECLARE v_split_coupon DECIMAL(16, 2);
    DECLARE v_order_price DECIMAL(10, 2);

    -- 用户游标
    DECLARE user_cur CURSOR FOR SELECT id FROM sync_test.user_info;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    OPEN user_cur;

    user_loop: LOOP
        FETCH user_cur INTO v_user_id;

        IF done THEN
            LEAVE user_loop;
        END IF;

        -- 随机取省份
        SELECT id INTO v_province_id
        FROM sync_test.base_province
        ORDER BY RAND()
        LIMIT 1;

        -- 订单明细数量 1~5
        SET v_detail_count = FLOOR(1 + RAND() * 5);
        SET v_total_amount = 0;
        SET v_original_total = 0;
        SET v_activity_amount = 0;
        SET v_coupon_amount = 0;

        -- 先生成订单主表
        INSERT INTO sync_test.order_info
        (consignee, consignee_tel, total_amount, order_status, user_id, payment_way,
         delivery_address, order_comment, out_trade_no, trade_body, create_time,
         operate_time, expire_time, process_status, tracking_no, parent_order_id,
         img_url, province_id, activity_reduce_amount, coupon_reduce_amount,
         original_total_amount, feight_fee, feight_fee_reduce, refundable_time)
        VALUES (CONCAT('收货人', FLOOR(1 + RAND() * 100)),
                CONCAT('138', LPAD(FLOOR(1 + RAND() * 99999999), 8, '0')),
                0,
                CASE FLOOR(1 + RAND() * 5)
                    WHEN 1 THEN 'UNPAID'
                    WHEN 2 THEN 'PAID'
                    WHEN 3 THEN 'SEND'
                    WHEN 4 THEN 'FINISHED'
                    ELSE 'CLOSED'
                    END,
                v_user_id,
                CASE FLOOR(1 + RAND() * 3)
                    WHEN 1 THEN 'ALIPAY'
                    WHEN 2 THEN 'WECHAT'
                    ELSE 'UNIONPAY'
                    END,
                CONCAT('地址', FLOOR(1 + RAND() * 1000)),
                CONCAT('备注', FLOOR(1 + RAND() * 100)),
                CONCAT('OUT', DATE_FORMAT(NOW(), '%Y%m%d%H%i%s'), FLOOR(1 + RAND() * 9999)),
                CONCAT('订单描述', FLOOR(1 + RAND() * 100)),
                NOW(),
                NOW(),
                DATE_ADD(NOW(), INTERVAL 30 MINUTE),
                CASE FLOOR(1 + RAND() * 3)
                    WHEN 1 THEN 'UNPAID'
                    WHEN 2 THEN 'PAID'
                    ELSE 'REFUND'
                    END,
                CONCAT('SF', LPAD(FLOOR(1 + RAND() * 999999999999), 12, '0')),
                NULL,
                NULL,
                v_province_id,
                0,
                0,
                0,
                ROUND(RAND() * 20, 2),
                0,
                DATE_ADD(NOW(), INTERVAL 30 DAY));

        SET v_order_id = LAST_INSERT_ID();

        -- 生成订单明细
        SET v_i = 0;
        WHILE v_i < v_detail_count DO
            -- 随机取SKU
            SELECT id, sku_name, sku_default_img, price
            INTO v_sku_id, v_sku_name, v_sku_img, v_sku_price
            FROM sync_test.sku_info
            ORDER BY RAND()
            LIMIT 1;

            SET v_sku_num = FLOOR(1 + RAND() * 10);
            SET v_order_price = v_sku_price;

            -- 计算分摊金额
            SET v_split_total = ROUND(v_order_price * v_sku_num, 2);
            SET v_split_activity = ROUND(v_split_total * RAND() * 0.1, 2);
            SET v_split_coupon = ROUND((v_split_total - v_split_activity) * RAND() * 0.05, 2);
            SET v_split_total = ROUND(v_split_total - v_split_activity - v_split_coupon, 2);

            INSERT INTO sync_test.order_detail
            (order_id, sku_id, sku_name, img_url, order_price, sku_num, create_time,
             split_total_amount, split_activity_amount, split_coupon_amount, operate_time)
            VALUES (
                    v_order_id,
                    v_sku_id,
                    v_sku_name,
                    v_sku_img,
                    v_order_price,
                    v_sku_num,
                    NOW(),
                    v_split_total,
                    v_split_activity,
                    v_split_coupon,
                    NOW());

            SET v_detail_id = LAST_INSERT_ID();

            -- 累计订单金额
            SET v_total_amount = v_total_amount + v_split_total;
            SET v_original_total = v_original_total + (v_order_price * v_sku_num);
            SET v_activity_amount = v_activity_amount + v_split_activity;
            SET v_coupon_amount = v_coupon_amount + v_split_coupon;

            -- 延迟2秒后生成活动和优惠券关联表
            DO SLEEP(2);

            -- 订单明细活动表（50%概率生成）
            IF RAND() > 0.5 THEN
                INSERT INTO sync_test.order_detail_activity
                (order_id, order_detail_id, activity_id, activity_rule_id, sku_id, create_time, operate_time)
                VALUES (
                    v_order_id,
                    v_detail_id,
                    FLOOR(1 + RAND() * 100),
                    FLOOR(1 + RAND() * 500),
                    v_sku_id,
                    NOW(),
                    NOW()
                );
            END IF;

            -- 订单明细优惠券表（50%概率生成）
            IF RAND() > 0.5 THEN
                INSERT INTO sync_test.order_detail_coupon
                (order_id, order_detail_id, coupon_id, coupon_use_id, sku_id, create_time, operate_time)
                VALUES (
                    v_order_id,
                    v_detail_id,
                    FLOOR(1 + RAND() * 200),
                    FLOOR(1 + RAND() * 1000),
                    v_sku_id,
                    NOW(),
                    NOW()
                );
            END IF;

            SET v_i = v_i + 1;
        END WHILE;

        -- 更新订单主表金额
        UPDATE sync_test.order_info
        SET total_amount = v_total_amount,
            original_total_amount = v_original_total,
            activity_reduce_amount = v_activity_amount,
            coupon_reduce_amount = v_coupon_amount
        WHERE id = v_order_id;

        -- 随机睡眠 3~5 秒，再生成下一个订单
        SET v_sleep_time = FLOOR(3 + RAND() * 3);
        DO SLEEP(v_sleep_time);

    END LOOP;

    CLOSE user_cur;
END //
DELIMITER ;

-- 执行
CALL InsertOrderWithDelay();