package cn.example.trade.sku.order.statistics.function.dimension;

import cn.example.common.demo.entity.TradeSkuOrderStatistics;
import com.alibaba.fastjson2.JSONObject;

public class Category3AsyncFunction extends DimAsyncFunction<TradeSkuOrderStatistics> {

    @Override
    public void addDims(TradeSkuOrderStatistics skuOrderStatistics, JSONObject dim) {
        skuOrderStatistics.setCategory3Name(dim.getString("name"));
        skuOrderStatistics.setCategory2Id(dim.getString("category2_id"));
    }

    @Override
    public String getTableName() {
        return "dim_base_category3";
    }

    @Override
    public String getRowKey(TradeSkuOrderStatistics skuOrderStatistics) {
        return skuOrderStatistics.getCategory3Id();
    }

}
