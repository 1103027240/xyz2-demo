package cn.example.trade.sku.order.statistics.function.dimension;

import cn.example.common.demo.entity.TradeSkuOrderStatistics;
import com.alibaba.fastjson2.JSONObject;

public class SpuInfoAsyncFunction extends DimAsyncFunction<TradeSkuOrderStatistics> {

    @Override
    public void addDims(TradeSkuOrderStatistics orderBean, JSONObject dimJsonObj) {
        orderBean.setSpuName(dimJsonObj.getString("spu_name"));
    }

    @Override
    public String getTableName() {
        return "dim_spu_info";
    }

    @Override
    public String getRowKey(TradeSkuOrderStatistics orderBean) {
        return orderBean.getSpuId();
    }

}
