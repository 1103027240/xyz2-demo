package cn.example.trade.sku.order.statistics.function.dimension;

import cn.example.common.demo.entity.TradeSkuOrderStatistics;
import com.alibaba.fastjson2.JSONObject;

public class SkuInfoAsyncFunction extends DimAsyncFunction<TradeSkuOrderStatistics> {

    @Override
    public void addDims(TradeSkuOrderStatistics orderBean, JSONObject dimJsonObj) {
        orderBean.setSkuName(dimJsonObj.getString("sku_name"));
        orderBean.setSpuId(dimJsonObj.getString("spu_id"));
        orderBean.setCategory3Id(dimJsonObj.getString("category3_id"));
        orderBean.setTrademarkId(dimJsonObj.getString("tm_id"));
    }

    @Override
    public String getTableName() {
        return "dim_sku_info";
    }

    @Override
    public String getRowKey(TradeSkuOrderStatistics orderBean) {
        return orderBean.getSkuId();
    }

}
