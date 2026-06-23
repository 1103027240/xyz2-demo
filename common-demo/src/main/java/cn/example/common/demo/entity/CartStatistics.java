package cn.example.common.demo.entity;

import com.alibaba.fastjson2.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.io.Serializable;
import java.util.Date;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class CartStatistics implements Serializable {
    private static final long serialVersionUID = 1L;

    // 当天日期
    @JSONField(format = "yyyy-MM-dd")
    private Date dt;

    @JSONField(format = "yyyy-MM-dd HH:mm:ss", name = "window_start")
    private Date windowStart;

    @JSONField(format = "yyyy-MM-dd HH:mm:ss", name = "window_end")
    private Date windowEnd;

    // 加购独立用户数
    @JSONField(name = "cart_add_cnt")
    private Long cartAddCnt;

}
