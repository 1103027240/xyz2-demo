package cn.example.dim.demo.function;

import cn.example.common.demo.constant.Constant;
import cn.example.common.demo.entity.TableProcessDim;
import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * 进行增删改查操作的配置表数据
 */
public class TableProcessDimSourceFunction implements MapFunction<String, TableProcessDim> {

    @Override
    public TableProcessDim map(String jsonStr) throws Exception {
        JSONObject jsonObject = JSONObject.parse(jsonStr);
        String op = jsonObject.getString(Constant.MYSQL_CDC_OP);

        TableProcessDim tableProcessDim;
        if (Constant.MYSQL_CDC_DELETE.equals(op)) {
            tableProcessDim = jsonObject.getObject(Constant.MYSQL_CDC_BEFORE, TableProcessDim.class);
        } else {
            tableProcessDim = jsonObject.getObject(Constant.MYSQL_CDC_AFTER, TableProcessDim.class);
        }

        tableProcessDim.setOp(op);
        return tableProcessDim;
    }

}
