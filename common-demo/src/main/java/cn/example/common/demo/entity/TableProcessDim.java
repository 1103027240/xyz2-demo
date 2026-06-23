package cn.example.common.demo.entity;

import com.alibaba.fastjson2.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TableProcessDim implements Serializable {
    private static final long serialVersionUID = 1L;

    // 来源表名
    @JSONField(name = "source_table")
    private String sourceTable;

    // 目标表名
    @JSONField(name = "sink_table")
    private String sinkTable;

    // HBase 列族
    @JSONField(name = "sink_family")
    private String sinkFamily;

    // HBase 列字段
    @JSONField(name = "sink_columns")
    private String sinkColumns;

    // HBase RowKey字段
    @JSONField(name = "sink_row_key")
    private String sinkRowKey;

    // 配置表操作类型
    private String op;

}
