package org.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DatabaseConnection {
    private static DatabaseConnection instance = null;
    private final String dbUrl;
    private final String user;
    private final String pass;

    private DatabaseConnection(String dbUrl, String user, String pass) {
        this.dbUrl = dbUrl;
        this.user = user;
        this.pass = pass;
    }

    public static DatabaseConnection getInstance(String dbUrl, String user, String pass) {
        if (instance == null) {
            instance = new DatabaseConnection(dbUrl, user, pass);
        }
        return instance;
    }

    public Connection getConnection() throws SQLException {
        Connection con = DriverManager.getConnection(dbUrl, user, pass);
        con.setAutoCommit(false);
        System.out.println("Conexión exitosa");
        return con;
    }
}
