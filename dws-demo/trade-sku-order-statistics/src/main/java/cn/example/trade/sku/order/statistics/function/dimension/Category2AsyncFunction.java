package cn.example.trade.sku.order.statistics.function.dimension;

import cn.example.common.demo.entity.TradeSkuOrderStatistics;
import cn.example.common.demo.function.DimAsyncFunction;
import com.alibaba.fastjson2.JSONObject;

public class Category2AsyncFunction extends DimAsyncFunction<TradeSkuOrderStatistics> {

    @Override
    public void addDims(TradeSkuOrderStatistics skuOrderStatistics, JSONObject dim) {
        skuOrderStatistics.setCategory2Name(dim.getString("name"));
        skuOrderStatistics.setCategory1Id(dim.getString("category1_id"));
    }

    @Override
    public String getTableName() {
        return "dim_base_category2";
    }

    @Override
    public String getRowKey(TradeSkuOrderStatistics skuOrderStatistics) {
        return skuOrderStatistics.getCategory2Id();
    }

}
