package cn.example.trade.sku.order.statistics.function.dimension;

import cn.example.common.demo.entity.TradeSkuOrderStatistics;
import com.alibaba.fastjson2.JSONObject;

public class Category3AsyncFunction extends DimAsyncFunction<TradeSkuOrderStatistics> {

    @Override
    public String getRowKey(TradeSkuOrderStatistics bean) {
        return bean.getCategory3Id();
    }

    @Override
    public String getTableName() {
        return "dim_base_category3";
    }

    @Override
    public void addDims(TradeSkuOrderStatistics bean, JSONObject dim) {
        bean.setCategory3Name(dim.getString("name"));
        bean.setCategory2Id(dim.getString("category2_id"));
    }

}
