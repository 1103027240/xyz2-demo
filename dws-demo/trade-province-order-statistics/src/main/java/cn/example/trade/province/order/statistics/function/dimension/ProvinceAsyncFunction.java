package cn.example.trade.province.order.statistics.function.dimension;

import cn.example.common.demo.entity.TradeProvinceOrderStatistics;
import cn.example.common.demo.function.DimAsyncFunction;
import com.alibaba.fastjson2.JSONObject;

public class ProvinceAsyncFunction extends DimAsyncFunction<TradeProvinceOrderStatistics> {

    @Override
    public void addDims(TradeProvinceOrderStatistics orderBean, JSONObject dimJsonObj) {
        orderBean.setProvinceName(dimJsonObj.getString("name"));
    }

    @Override
    public String getTableName() {
        return "dim_base_province";
    }

    @Override
    public String getRowKey(TradeProvinceOrderStatistics orderBean) {
        return orderBean.getProvinceId();
    }

}
