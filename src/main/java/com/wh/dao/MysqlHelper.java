package com.wh.dao;

import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.*;

public class MysqlHelper {
    private static Connection conn = null;
    private PreparedStatement ps = null;
    private ResultSet rs = null;

    private static String driver = "";
    private static String url = "";
    private static String userName = "";
    private static String password = "";

    private static Properties pp = null;
    private static InputStream fis = null;

    public Connection getConn() {
        return conn;
    }

    public PreparedStatement getPs() {
        return ps;
    }

    public ResultSet getRs() {
        return rs;
    }


    static {
        try {
            pp = new Properties();
            fis = MysqlHelper.class.getResourceAsStream("/db.properties");
            pp.load(fis);
            driver = pp.getProperty("jdbc.mysql.driver");
            url = pp.getProperty("jdbc.mysql.url");
            userName = pp.getProperty("jdbc.mysql.username");
            password = pp.getProperty("jdbc.mysql.password");

            Class.forName(driver);
            conn = DriverManager.getConnection(url, userName, password);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (fis != null)
                try {
                    fis.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            fis = null;

        }
    }

    public Connection getConnection() {
        try {
            conn = DriverManager.getConnection(url, userName, password);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }

    public void executeBatch(String sql, List<String[]> params) {
        try {
//            conn = getConnection();
            conn.setAutoCommit(false);
            PreparedStatement ps = conn.prepareStatement(sql);
            int count = 0;
            for (int i = 0; i < params.size(); i++) {
                String[] param = params.get(i);
                if (param != null) {
//                    PreparedStatement ps = conn.prepareStatement(sql);
                    for (int j = 0; j < param.length; j++)
                        ps.setString(j + 1, param[j]);
                }
                ps.addBatch();
                count++;
                if (count >= 500) {
                    ps.executeBatch();
                    ps.clearBatch();
                    count = 0;
                    System.out.println("500 lines execute.");
                }
            }
            ps.executeBatch();
            conn.commit();
        } catch (Exception e) {
            e.printStackTrace();
            try {
                conn.rollback();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
            throw new RuntimeException(e.getMessage());
        } finally {
//            close(rs, ps, conn);
        }
    }

    public void execute(String sql, String[] parameters) {
        try {
//            conn = getConnection();
            PreparedStatement ps = conn.prepareStatement(sql);
            if (parameters != null)
                for (int i = 0; i < parameters.length; i++) {
                    ps.setString(i + 1, parameters[i]);
                }
            ps.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        } finally {
//            close(rs, ps, conn);
        }
    }

    // query select
    public List<Map<String, Object>> query(String sql, String[] parameters) {
        List list = new ArrayList();
        ResultSet rs = null;
        try {
//            conn = getConnection();
            PreparedStatement ps = conn.prepareStatement(sql);
            if (parameters != null) {
                for (int i = 0; i < parameters.length; i++) {
                    ps.setString(i + 1, parameters[i]);
                }
            }
            rs = ps.executeQuery();
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnCount = rsmd.getColumnCount();
            while (rs.next()) {
                Map rowData = new HashMap();
                for (int i = 1; i <= columnCount; i++) {
                    rowData.put(rsmd.getColumnName(i), rs.getObject(i));
                }
                list.add(rowData);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }
        return list;
    }


    public void close() {
        if (rs != null)
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        rs = null;
        if (ps != null)
            try {
                ps.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        ps = null;
        if (conn != null)
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        conn = null;
    }
}