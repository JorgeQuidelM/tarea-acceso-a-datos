package org.example;

import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public class DatabaseManager {
    private final Connection conexion;
    private final ReentrantLock lock = new ReentrantLock();

    public DatabaseManager(Connection conexion) {
        this.conexion = conexion;
    }

    public List<String> getSchemas() throws DatabaseException {
        lock.lock();
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
        } finally {
            lock.unlock();
        }
        return listaEsquemas;
    }

    public List<Record> selectAll(String schemaName, String tableName) throws DatabaseException {
        lock.lock();
        String query = "SELECT * FROM " + schemaName + "." + tableName;
        List<Record> records;
        try (Statement statement = conexion.createStatement();
             ResultSet resultSet = statement.executeQuery(query)) {
            records = buildRecordsFromResultSet(schemaName, tableName, resultSet);
        } catch (SQLException errSql) {
            throw new DatabaseException("Error SQL al intentar obtener los registros: " + errSql.getMessage(), errSql);
        }
        lock.unlock();
        return records;
    }

    private List<Record> buildRecordsFromResultSet(String schemaName, String tableName, ResultSet resultSet) throws SQLException, DatabaseException {
        List<String> columnNames = getColumnsNames(schemaName, tableName);
        List<Record> records = new ArrayList<>();
        while (resultSet.next()) {
            Record record = new Record();
            for (String columnName : columnNames) {
                String columnType = getColumnType(schemaName, tableName, columnName);
                record.addColumnValue(columnName, resultSet.getString(columnName), columnType);
            }
            records.add(record);
        }
        return records;
    }

    public void insertRecord(String schemaName, String tableName, Record record) throws DatabaseException {
        lock.lock();
        String query = getInsertQuery(schemaName, tableName, record);
        try (PreparedStatement statement = conexion.prepareStatement(query)) {
            int i = 0;
            for (String columnName : record.getColumnNames()) {
                setParameterValue(statement, i + 1, record.getDataType(columnName), record.getValue(columnName));
                i++;
            }
            int rowsAffected = statement.executeUpdate();
            conexion.commit();
            System.out.println(rowsAffected + " fila(s) insertada(s).");
        } catch (SQLException errSql) {
            rollbackOnSQLException();
            throw new DatabaseException("Error SQL al intentar agregar un registro. " + errSql.getMessage(), errSql);
        } catch (ParseException errPar) {
            throw new DatabaseException("Error Parse al intentar persear un valor de la columna. " + errPar.getMessage(), errPar);
        } catch (IllegalArgumentException errIll){
            throw new DatabaseException("Error IllegalArgument al reconoce el tipo de dato. " + errIll.getMessage(), errIll);
        } finally {
            lock.unlock();
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
        lock.lock();
        List<String> whereColumns = whereRecord.getColumnNames();
        String query = getDeleteQuery(schemaName, tableName, whereRecord);
        try (PreparedStatement statement = conexion.prepareStatement(query)) {
            for (String columnName : whereColumns) {
                setParameterValue(statement,
                        whereColumns.indexOf(columnName) + 1,
                        whereRecord.getDataType(columnName),
                        whereRecord.getValue(columnName));
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
        } finally {
            lock.unlock();
        }
    }

    public String getColumnType(String schemaName, String tableName, String columnName) throws DatabaseException {
        List<String> columnTypes = getColumnsTypes(schemaName, tableName);
        List<String> columnNames = getColumnsNames(schemaName, tableName);

        int columnIndex = columnNames.indexOf(columnName);
        if (columnIndex != -1) {
            return columnTypes.get(columnIndex);
        } else {
            throw new DatabaseException("La columna '" + columnName + "' no existe en la tabla '" + tableName + "'.");
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
        lock.lock();
        List<String> whereColumns = whereRecord.getColumnNames();
        String query = getUpdateQuery(schemaName, tableName, columnName, whereRecord);
        try (PreparedStatement statement = conexion.prepareStatement(query)) {
            String columnType = getColumnType(schemaName, tableName, columnName);
            setParameterValue(statement, 1, columnType, newValue);
            for (String whereColumn : whereColumns) {
                setParameterValue(statement,
                        whereColumns.indexOf(whereColumn) + 2,
                        whereRecord.getDataType(whereColumn),
                        whereRecord.getValue(whereColumn));
            }
            int rowsAffected = statement.executeUpdate();
            conexion.commit();
            System.out.println(rowsAffected + " fila(s) actualizada(s).");
        } catch (SQLException errSql) {
            throw new DatabaseException("Error SQL al intentar modificar un registro: " + errSql.getMessage(), errSql);
        } catch (ParseException errPar) {
            throw new DatabaseException("Error Parse al intentar persear un valor de la columna" + errPar.getMessage(), errPar);
        } catch (IllegalArgumentException errIll){
            throw new DatabaseException("Error IllegalArgument al reconoce el tipo de dato" + errIll.getMessage(), errIll);
        } finally {
            lock.unlock();
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

    private List<String> getColumnMetadata(String schemaName, String tableName, String columnName) throws DatabaseException {
        lock.lock();
        List<String> metadataList = new ArrayList<>();
        String query = "SELECT " + columnName +
                " FROM information_schema.columns " +
                " WHERE table_schema = ? AND table_name = ?";

        try (PreparedStatement statement = conexion.prepareStatement(query)) {
            statement.setString(1, schemaName);
            statement.setString(2, tableName);

            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    String metadata = resultSet.getString(columnName);
                    metadataList.add(metadata);
                }
            }
        } catch (SQLException errSql) {
            throw new DatabaseException("Error SQL al intentar obtener " + columnName + ": " + errSql.getMessage(), errSql);
        } finally {
            lock.unlock();
        }
        return metadataList;
    }

    public List<String> getColumnsNames(String schemaName, String tableName) throws DatabaseException {
        return getColumnMetadata(schemaName, tableName, "column_name");
    }

    public List<String> getColumnsTypes(String schemaName, String tableName) throws DatabaseException {
        return getColumnMetadata(schemaName, tableName, "data_type");
    }

    public List<String> getColumnValues(String schemaName, String tableName, String columnName) throws DatabaseException {
        lock.lock();
        String query = "SELECT " + columnName + " FROM " + schemaName + "." + tableName;
        List<String> values = new ArrayList<>();
        try (Statement statement = conexion.createStatement();
             ResultSet resultSet = statement.executeQuery(query)) {
            while (resultSet.next()) {
                values.add(resultSet.getString(columnName));
            }
        } catch (SQLException errSql) {
            throw new DatabaseException("Error SQL al intentar obtener los valores de una columna: " + errSql.getMessage(), errSql);
        } finally {
            lock.unlock();
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
