package cn.example.common.demo.entity;

import com.alibaba.fastjson2.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Date;
import java.util.Set;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TradeProvinceOrderStatistics implements Serializable {
    private static final long serialVersionUID = 1L;

    // 当天日期
    @JSONField(format = "yyyy-MM-dd")
    private Date dt;

    @JSONField(format = "yyyy-MM-dd HH:mm:ss", name = "window_start")
    private Date windowStart;

    @JSONField(format = "yyyy-MM-dd HH:mm:ss", name = "window_end")
    private Date windowEnd;

    // 省份 ID
    @JSONField(name = "province_id")
    private String provinceId;

    // 省份名称
    @Builder.Default
    @JSONField(name = "province_name")
    private String provinceName = "";

    // 累计下单次数
    @JSONField(name = "order_count")
    private Long orderCount;

    // 累计下单金额
    @JSONField(name = "order_amount")
    private BigDecimal orderAmount;

    // 时间戳
    @JSONField(serialize = false)
    private Long ts;

    @JSONField(serialize = false)
    private Set<String> orderIdSet;

}
