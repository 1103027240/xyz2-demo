package cn.example.trade.sku.order.statistics.function.dimension;

import cn.example.common.demo.entity.TradeSkuOrderStatistics;
import cn.example.common.demo.function.DimAsyncFunction;
import com.alibaba.fastjson2.JSONObject;

public class TradeMarkAsyncFunction extends DimAsyncFunction<TradeSkuOrderStatistics> {

    @Override
    public void addDims(TradeSkuOrderStatistics skuOrderStatistics, JSONObject dimJsonObj) {
        skuOrderStatistics.setTrademarkName(dimJsonObj.getString("tm_name"));
    }

    @Override
    public String getTableName() {
        return "dim_base_trademark";
    }

    @Override
    public String getRowKey(TradeSkuOrderStatistics skuOrderStatistics) {
        return skuOrderStatistics.getTrademarkId();
    }

}
