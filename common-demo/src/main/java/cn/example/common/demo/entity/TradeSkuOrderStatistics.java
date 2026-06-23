package cn.example.common.demo.entity;

import com.alibaba.fastjson2.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Date;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TradeSkuOrderStatistics implements Serializable {
    private static final long serialVersionUID = 1L;

    // 当天日期
    @JSONField(format = "yyyy-MM-dd")
    private Date dt;

    @JSONField(format = "yyyy-MM-dd HH:mm:ss", name = "window_start")
    private Date windowStart;

    @JSONField(format = "yyyy-MM-dd HH:mm:ss", name = "window_end")
    private Date windowEnd;

    // 品牌 ID
    @JSONField(name = "trade_mark_id")
    private String trademarkId;

    // 品牌名称
    @JSONField(name = "trade_mark_name")
    private String trademarkName;

    // 一级品类 ID
    @JSONField(name = "category1_id")
    private String category1Id;

    // 一级品类名称
    @JSONField(name = "category1_name")
    private String category1Name;

    // 二级品类 ID
    @JSONField(name = "category2_id")
    private String category2Id;

    // 二级品类名称
    @JSONField(name = "category2_name")
    String category2Name;

    // 三级品类 ID
    @JSONField(name = "category3_id")
    private String category3Id;

    // 三级品类名称
    @JSONField(name = "category3_name")
    private String category3Name;

    // sku_id
    @JSONField(name = "sku_id")
    private String skuId;

    // sku 名称
    @JSONField(name = "sku_name")
    private String skuName;

    // spu_id
    @JSONField(name = "spu_id")
    private String spuId;

    // spu 名称
    @JSONField(name = "spu_name")
    private String spuName;

    // 原始金额
    @JSONField(name = "original_amount")
    private BigDecimal originalAmount;

    // 活动减免金额
    @JSONField(name = "activity_reduce_amount")
    private BigDecimal activityReduceAmount;

    // 优惠券减免金额
    @JSONField(name = "coupon_reduce_amount")
    private BigDecimal couponReduceAmount;

    // 下单金额
    @JSONField(name = "order_amount")
    private BigDecimal orderAmount;

    // 时间戳
    @JSONField(serialize = false)
    private Long ts;

}
