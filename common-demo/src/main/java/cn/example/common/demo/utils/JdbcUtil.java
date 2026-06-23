package cn.example.common.demo.utils;

import cn.example.common.demo.constant.Constant;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class JdbcUtil {

    //获取MySQL连接
    public static Connection getMySQLConnection() throws Exception {
        Class.forName("com.mysql.cj.jdbc.Driver");
        Connection conn = DriverManager.getConnection(Constant.MYSQL_URL, Constant.MYSQL_USERNAME, Constant.MYSQL_PASSWORD);
        return conn;

    }

    //关闭MySQL连接
    public static void closeMySQLConnection(Connection conn) throws SQLException {
        if (conn != null && !conn.isClosed()) {
            conn.close();
        }
    }

    //查询数据
    public static <T> List<T> queryList(Connection conn, String sql, Class<T> clz, boolean... isUnderlineToCamel) throws Exception {
        List<T> resList = new ArrayList<>();

        boolean defaultIsUToC = false;  //默认不执行下划线转驼峰
        if (isUnderlineToCamel.length > 0) {
            defaultIsUToC = isUnderlineToCamel[0];
        }

        PreparedStatement ps = conn.prepareStatement(sql);
        ResultSet rs = ps.executeQuery();
        ResultSetMetaData metaData = rs.getMetaData();
        while (rs.next()) {
            T obj = clz.newInstance();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                String columnName = metaData.getColumnName(i);
                Object columnValue = rs.getObject(i);
                if (defaultIsUToC) {
                    columnName = CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
                }
                BeanUtils.setProperty(obj, columnName, columnValue);
            }
            resList.add(obj);
        }

        return resList;
    }
}
