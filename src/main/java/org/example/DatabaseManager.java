package org.example;

import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DatabaseManager {
    private final Connection conexion;

    public DatabaseManager(Connection conexion) {
        this.conexion = conexion;
    }

    public List<String> getSchemas() throws DatabaseException {
        List<String> listaEsquemas = new ArrayList<>();
        String query = "SELECT nspname AS schema_name " +
                "FROM pg_catalog.pg_namespace " +
                "WHERE nspname <> 'pg_toast' AND nspname !~ '^pg_' " +
                "AND nspname <> 'information_schema';";
        try (Statement statement = conexion.createStatement();
             ResultSet resultSet = statement.executeQuery(query)) {
            System.out.println("Listado de esquemas en la base de datos:");
            while (resultSet.next()) {
                String tableName = resultSet.getString("schema_name");
                listaEsquemas.add(tableName);
            }
        } catch (SQLException errSql) {
            throw new DatabaseException("Error SQL al intentar obtener los registros: " + errSql.getMessage(), errSql);
        }
        return listaEsquemas;
    }

    public List<Record> selectAll(String schemaName, String tableName, List<String> columnNames) throws DatabaseException {
        String query = "SELECT * FROM " + schemaName + "." + tableName;
        List<Record> records;
        try (Statement statement = conexion.createStatement();
             ResultSet resultSet = statement.executeQuery(query)) {
            records = buildRecordsFromResultSet(resultSet, columnNames);
        } catch (SQLException errSql) {
            throw new DatabaseException("Error SQL al intentar obtener los registros: " + errSql.getMessage(), errSql);
        }
        return records;
    }

    public List<Record> selectColumns(String tableName, List<String> columnNames) throws DatabaseException {
        String query = "SELECT " + String.join(", ", columnNames) + " FROM " + tableName;
        List<Record> records;
        try (Statement statement = conexion.createStatement();
             ResultSet resultSet = statement.executeQuery(query)) {
            records = buildRecordsFromResultSet(resultSet, columnNames);
        } catch (SQLException errSql) {
            throw new DatabaseException("Error SQL al intentar obtener los registros: " + errSql.getMessage(), errSql);
        }
        return records;
    }

    private List<Record> buildRecordsFromResultSet(ResultSet resultSet, List<String> columnNames) throws SQLException {
        List<Record> records = new ArrayList<>();
        while (resultSet.next()) {
            Record record = new Record();
            for (String columnName : columnNames) {
                record.addColumnValue(columnName, resultSet.getString(columnName));
            }
            records.add(record);
        }
        return records;
    }

    public void insertRecord(String schemaName, String tableName, Record record) throws DatabaseException {
        String query = getInsertQuery(schemaName, tableName, record);
        List<String> dataTypes = getColumnsTypes(schemaName, tableName);
        try (PreparedStatement statement = conexion.prepareStatement(query)) {
            int i = 0;
            for (String columnName : record.getColumnNames()) {
                setParameterValue(statement, i + 1, dataTypes.get(i), record.getValue(columnName));
                i++;
            }
            int rowsAffected = statement.executeUpdate();
            conexion.commit();
            System.out.println(rowsAffected + " fila(s) insertada(s).");
        } catch (SQLException errSql) {
            rollbackOnSQLException();
            throw new DatabaseException("Error SQL al intentar agregar un registro: " + errSql.getMessage(), errSql);
        } catch (ParseException errPar) {
            throw new DatabaseException("Error Parse al intentar persear un valor de la columna" + errPar.getMessage(), errPar);
        } catch (IllegalArgumentException errIll){
            throw new DatabaseException("Error IllegalArgument al reconoce el tipo de dato" + errIll.getMessage(), errIll);
        }
    }

    private void setParameterValue(PreparedStatement statement, int index, String dataType, String value) throws SQLException, ParseException {
        switch (dataType.toLowerCase()) {
            case "integer", "big int" -> statement.setInt(index, Integer.parseInt(value));
            case "character", "character varying" -> statement.setString(index, value);
            case "boolean" -> statement.setBoolean(index, Boolean.parseBoolean(value));
            case "date" -> {
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
                java.util.Date parsedDate = dateFormat.parse(value);
                statement.setDate(index, new Date(parsedDate.getTime()));
            }
            default -> throw new IllegalArgumentException("Tipo de dato no soportado para el tipo: " + dataType);
        }
    }

    private String getInsertQuery(String schemaName, String tableName, Record record) {
        List<String> columnNames = record.getColumnNames();
        String columns = String.join(", ", columnNames);
        String values = String.join(", ", Collections.nCopies(columnNames.size(), "?"));
        return String.format("INSERT INTO %s.%s (%s) VALUES (%s)", schemaName, tableName, columns, values);
    }

    public void deleteRecord(String schemaName, String tableName, Record whereRecord) throws DatabaseException {
        List<String> whereColumns = whereRecord.getColumnNames();
        List<String> dataTypes = getColumnsTypes(schemaName, tableName);
        String query = getDeleteQuery(schemaName, tableName, whereRecord);
        try (PreparedStatement statement = conexion.prepareStatement(query)) {
            int i = 0;
            for (String columnName : whereColumns) {
                setParameterValue(statement, i + 1, dataTypes.get(i), whereRecord.getValue(columnName));
                i++;
            }
            int rowsAffected = statement.executeUpdate();
            conexion.commit();
            System.out.println(rowsAffected + " fila(s) eliminada(s).");
        } catch (SQLException errSql) {
            rollbackOnSQLException();
            throw new DatabaseException("Error SQL al intentar eliminar un registro: " + errSql.getMessage(), errSql);
        } catch (ParseException errPar) {
            throw new DatabaseException("Error Parse al intentar persear un valor de la columna" + errPar.getMessage(), errPar);
        } catch (IllegalArgumentException errIll){
            throw new DatabaseException("Error IllegalArgument al reconoce el tipo de dato" + errIll.getMessage(), errIll);
        }
    }

    private String getDeleteQuery(String schemaName, String tableName, Record whereRecord) {
        StringBuilder queryBuilder = new StringBuilder("DELETE FROM ").append(schemaName).append(".").append(tableName);
        List<String> whereColumns = whereRecord.getColumnNames();
        queryBuilder.append(" WHERE ");
        for (int i = 0; i < whereColumns.size(); i++) {
            String separator = (i == 0) ? "" : " AND ";
            queryBuilder.append(separator).append(whereColumns.get(i)).append(" = ?");
        }
        return queryBuilder.toString();
    }

    public void updateRecord(String schemaName, String tableName, String columnName, String newValue, Record whereRecord) throws DatabaseException {
        List<String> whereColumns = whereRecord.getColumnNames();
        List<String> dataTypes = getColumnsTypes(schemaName, tableName);
        String query = getUpdateQuery(schemaName, tableName, columnName, whereRecord);
        try (PreparedStatement statement = conexion.prepareStatement(query)) {
            statement.setString(1, newValue);
            int j = 2;
            for (String column : whereColumns) {
                setParameterValue(statement, j, dataTypes.get(j),whereRecord.getValue(column) );
                j++;
            }
            int rowsAffected = statement.executeUpdate();
            System.out.println(rowsAffected + " fila(s) actualizada(s).");
        } catch (SQLException errSql) {
            throw new DatabaseException("Error SQL al intentar modificar un registro: " + errSql.getMessage(), errSql);
        } catch (ParseException errPar) {
            throw new DatabaseException("Error Parse al intentar persear un valor de la columna" + errPar.getMessage(), errPar);
        } catch (IllegalArgumentException errIll){
            throw new DatabaseException("Error IllegalArgument al reconoce el tipo de dato" + errIll.getMessage(), errIll);
        }
    }

    private String getUpdateQuery(String schemaName, String tableName, String columnName, Record whereRecord) {
        StringBuilder queryBuilder = new StringBuilder("UPDATE ").append(schemaName).append(".").append(tableName).append(" SET ").append(columnName).append(" = ?");
        List<String> whereColumns = whereRecord.getColumnNames();
        for (int i = 0; i < whereColumns.size(); i++) {
            String separator = (i == 0) ? " WHERE " : " AND ";
            queryBuilder.append(separator).append(whereColumns.get(i)).append(" = ?");
        }
        return queryBuilder.toString();
    }

    public List<String> getTables(String schemaName) throws DatabaseException {
        List<String> tableNames = new ArrayList<>();
        String query = "SELECT table_name " +
                "FROM information_schema.tables " +
                "WHERE table_schema = '" + schemaName + "' " +
                "AND table_type = 'BASE TABLE';";
        try (Statement statement = conexion.createStatement();
             ResultSet resultSet = statement.executeQuery(query)) {
            while (resultSet.next()) {
                String tableName = resultSet.getString(1);
                tableNames.add(tableName);
            }
        } catch (SQLException errSql) {
            throw new DatabaseException("Error SQL al intentar obtener las tablas: " + errSql.getMessage(), errSql);
        }
        return tableNames;
    }

    public List<String> getColumnsNames(String schemaName, String tableName) throws DatabaseException {
        List<String> columnNames = new ArrayList<>();
        String query = "SELECT column_name " +
                "FROM information_schema.columns " +
                "WHERE table_schema = '" + schemaName + "' " +
                "AND table_name = '" + tableName + "';";
        try (Statement statement = conexion.createStatement();
             ResultSet columns = statement.executeQuery(query)) {

            while (columns.next()) {
                String columnName = columns.getString("column_name");
                columnNames.add(columnName);
            }
        } catch (SQLException errSql) {
            throw new DatabaseException("Error SQL al intentar obtener las columnas: " + errSql.getMessage(), errSql);
        }
        return columnNames;
    }

    public List<String> getColumnsTypes(String schemaName, String tableName) throws DatabaseException {
        List<String> columnTypes = new ArrayList<>();
        String query = "SELECT data_type " +
                "FROM information_schema.columns " +
                "WHERE table_schema = '" + schemaName + "' " +
                "AND table_name = '" + tableName + "'";
        try (Statement statement = conexion.createStatement();
             ResultSet resultSet = statement.executeQuery(query)) {

            while (resultSet.next()) {
                String columnType = resultSet.getString("data_type");
                columnTypes.add(columnType);
            }
        } catch (SQLException errSql) {
            throw new DatabaseException("Error SQL al intentar obtener los tipos de datos de las columnas: " + errSql.getMessage(), errSql);
        }
        return columnTypes;
    }

    public List<String> getColumnValues(String schemaName, String tableName, String columnName) throws DatabaseException {
        String query = "SELECT " + columnName + " FROM " + schemaName + "." + tableName;
        List<String> values = new ArrayList<>();
        try (Statement statement = conexion.createStatement();
             ResultSet resultSet = statement.executeQuery(query)) {
            while (resultSet.next()) {
                values.add(resultSet.getString(columnName));
            }
        } catch (SQLException errSql) {
            throw new DatabaseException("Error SQL al intentar obtener los valores de una columna: " + errSql.getMessage(), errSql);
        }
        return values;
    }

    private void rollbackOnSQLException() throws DatabaseException {
        try {
            if (conexion != null) {
                conexion.rollback();
            }
        } catch (SQLException errRoll) {
            throw new DatabaseException("Error de rollback: " + errRoll.getMessage(), errRoll);
        }
    }
}
