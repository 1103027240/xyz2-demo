package cn.example.trade.sku.order.statistics.function.dimension;

import cn.example.common.demo.entity.TradeSkuOrderStatistics;
import com.alibaba.fastjson2.JSONObject;

public class SkuInfoAsyncFunction extends DimAsyncFunction<TradeSkuOrderStatistics> {

    @Override
    public void addDims(TradeSkuOrderStatistics skuOrderStatistics, JSONObject dimJsonObj) {
        skuOrderStatistics.setSkuName(dimJsonObj.getString("sku_name"));
        skuOrderStatistics.setSpuId(dimJsonObj.getString("spu_id"));
        skuOrderStatistics.setCategory3Id(dimJsonObj.getString("category3_id"));
        skuOrderStatistics.setTrademarkId(dimJsonObj.getString("tm_id"));
    }

    @Override
    public String getTableName() {
        return "dim_sku_info";
    }

    @Override
    public String getRowKey(TradeSkuOrderStatistics skuOrderStatistics) {
        return skuOrderStatistics.getSkuId();
    }

}
