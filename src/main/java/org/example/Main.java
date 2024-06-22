package org.example;

import java.sql.*;

public class Main {
    public static void main(String[] args) {
        String dbName = "";
        String dbUrl = "jdbc:postgresql://localhost/" + dbName;
        String user = "";
        String pass = "";

        try (Connection conexion = DatabaseConnection.getInstance(dbUrl, user, pass).getConnection()) {
            DatabaseManager database = new DatabaseManager(conexion);
            Menu menu = new Menu(database);
            menu.showSchemasMenu();
        } catch (SQLException errCon) {
            System.out.println("Error de conexión con la base de datos: " + errCon.getMessage());
        }

    }
}