package cn.example.trade.sku.order.statistics.function.dimension;

import cn.example.common.demo.entity.TradeSkuOrderStatistics;
import com.alibaba.fastjson2.JSONObject;

public class TradeMarkAsyncFunction extends DimAsyncFunction<TradeSkuOrderStatistics> {

    @Override
    public void addDims(TradeSkuOrderStatistics orderBean, JSONObject dimJsonObj) {
        orderBean.setTrademarkName(dimJsonObj.getString("tm_name"));
    }

    @Override
    public String getTableName() {
        return "dim_base_trademark";
    }

    @Override
    public String getRowKey(TradeSkuOrderStatistics orderBean) {
        return orderBean.getTrademarkId();
    }

}
