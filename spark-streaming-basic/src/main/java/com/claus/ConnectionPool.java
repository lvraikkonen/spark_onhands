package com.claus;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * 〈一句话功能简述〉<br> 
 * 〈implementation of Connection Pool〉
 *
 * @author lvshuo
 * @create 2018/8/30
 * @since 1.0.0
 */
public class ConnectionPool {

    private static ComboPooledDataSource dataSource = new ComboPooledDataSource();

    static {
        dataSource.setJdbcUrl("jdbc:mysql://localhost:3306/spark-test");
        dataSource.setUser("root");
        dataSource.setPassword("LvRaikkonen_0306");
        dataSource.setMaxPoolSize(50);
        dataSource.setMinPoolSize(2);
        dataSource.setInitialPoolSize(10);
        dataSource.setMaxStatements(100);
    }

    public static Connection getConnection(){
        try{
            return dataSource.getConnection();
        } catch(SQLException e){
            e.printStackTrace();
        }
        return null;
    }

    public static void returnConnection(Connection conn){
        if (conn != null){
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

}