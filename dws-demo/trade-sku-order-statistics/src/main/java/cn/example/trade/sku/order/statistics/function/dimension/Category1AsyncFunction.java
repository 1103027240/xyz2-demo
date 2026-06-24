package cn.example.trade.sku.order.statistics.function.dimension;

import cn.example.common.demo.entity.TradeSkuOrderStatistics;
import com.alibaba.fastjson2.JSONObject;

public class Category1AsyncFunction extends DimAsyncFunction<TradeSkuOrderStatistics> {

    @Override
    public void addDims(TradeSkuOrderStatistics skuOrderStatistics, JSONObject dim) {
        skuOrderStatistics.setCategory1Name(dim.getString("name"));
    }

    @Override
    public String getTableName() {
        return "dim_base_category1";
    }

    @Override
    public String getRowKey(TradeSkuOrderStatistics skuOrderStatistics) {
        return skuOrderStatistics.getCategory1Id();
    }

}
