package cn.example.trade.sku.order.statistics.function.dimension;

import cn.example.common.demo.entity.TradeSkuOrderStatistics;
import com.alibaba.fastjson2.JSONObject;

public class Category2AsyncFunction extends DimAsyncFunction<TradeSkuOrderStatistics> {

    @Override
    public String getRowKey(TradeSkuOrderStatistics bean) {
        return bean.getCategory2Id();
    }

    @Override
    public String getTableName() {
        return "dim_base_category2";
    }

    @Override
    public void addDims(TradeSkuOrderStatistics bean, JSONObject dim) {
        bean.setCategory2Name(dim.getString("name"));
        bean.setCategory1Id(dim.getString("category1_id"));
    }

}
