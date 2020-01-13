package com.dgut.hadoop_sample;

import org.datanucleus.store.rdbms.JDBCUtils;

import java.sql.*;

/**
 * @author linwt
 * @date 2019/11/28 10:23
 */
public class JdbcTemplate {
    private static String JDBC_DRIVER = "org.apache.hive.jdbc.HiveDriver";
    private static String CONNECTION_URL ="jdbc:hive2://centos-l1.niracler.com:10000/";

    static {
        try {
            Class.forName(JDBC_DRIVER);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws SQLException {
        System.out.println("通过JDBC连接非Kerberos环境下的HiveServer2");
        Connection connection = null;
        ResultSet rs = null;
        PreparedStatement ps = null;
        try {
            connection = DriverManager.getConnection(CONNECTION_URL);
            ps = connection.prepareStatement("select * from test_log_orc");
            rs = ps.executeQuery();
            int i = 0;
            while (rs.next() && i++ < 10) {
                System.out.println(rs.getString(3));
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            connection.close();
            rs.close();
            ps.close();
        }
    }

}
