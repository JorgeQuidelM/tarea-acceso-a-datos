package org.example;

import java.util.Arrays;
import java.util.List;

public class Menu {
    private final DatabaseManager database;

    public Menu(DatabaseManager database) {
        this.database = database;
    }

    public void startMenu(){
        showSchemasMenu();
    }

    public void showSchemasMenu() {
        List<String> schemaNames = getSchemaNames();
        if(schemaNames == null){
            return;
        }
        ConsoleUtils.printNumberedTable("Esquemas disponibes: ", schemaNames);
        int schemaIndex = askSchemaIndex(schemaNames);
        if(schemaIndex == -1){
            return;
        }
        String schemaSelected = schemaNames.get(schemaIndex);
        showTablesMenu(schemaSelected);
    }

    public void showTablesMenu(String schemaName) {
        List<String> tablesNames = getTablesNames(schemaName);
        if(tablesNames == null){
            return;
        }
        ConsoleUtils.printNumberedTable("Tablas disponibles: ", tablesNames);
        int tableIndex = askTableIndex(tablesNames);
        if(tableIndex == -1){
            return;
        }
        String tableSelected = tablesNames.get(tableIndex);
        showColumnsMenu(schemaName, tableSelected);
    }

    private void showColumnsMenu(String schemaName, String tableSelected) {
        List<String> columnNames = getColumnNames(schemaName, tableSelected);
        if (columnNames == null || columnNames.isEmpty()) {
            return;
        }
        int option;
        do {
            option = showTableOptionsMenu();
            executeOption(option, schemaName, tableSelected, columnNames);
        } while (option != 5);
    }

    private void executeOption(int option, String schemaName, String tableSelected, List<String> columnNames) {
        switch (option) {
            case 1 -> handleSelectAllRecords(schemaName, tableSelected);
            case 2 -> handleUpdateRecord(schemaName, tableSelected, columnNames);
            case 3 -> handleDeleteRecord(schemaName, tableSelected, columnNames);
            case 4 -> handleAddNewRecord(schemaName, tableSelected, columnNames);
            case 5 -> System.out.println("Saliendo de la aplicación...");
            default -> System.out.println("Opción inválida");
        }
    }

    private void handleSelectAllRecords(String schemaName, String tableName) {
        List<Record> records = getAllRecords(schemaName, tableName);
        if(records == null || records.isEmpty()){
            System.out.println("No se encontraron registros");
            return;
        }
        List<String> items = records.stream().map(Record::toString).toList();
        ConsoleUtils.printMarkedTable("Registros: ", items, "*");
    }

    private void handleUpdateRecord(String schemaName, String tableName, List<String> columnNames) {
        String whereColumn = getColumnName(tableName, columnNames, "filtrar (WHERE)");
        String whereValue = getWhereValue(schemaName, tableName, whereColumn);
        String whereType = getColumnType(schemaName, tableName, whereColumn);
        if (whereValue == null || whereType == null) {
            return;
        }
        Record whereRecord = new Record();
        whereRecord.addColumnValue(whereColumn, whereValue, whereType);
        String columnNameForNewValue = getColumnName(tableName, columnNames, "modificar (SET)");
        String newValue = ConsoleUtils.getStringInput("Ingrese el nuevo valor para " + columnNameForNewValue + ": ");
        updateRecord(schemaName, tableName, columnNameForNewValue, newValue, whereRecord);
    }

    private void handleDeleteRecord(String schemaName, String tableName, List<String> columnNames) {
        String whereColumn = getColumnName(tableName, columnNames, "filtrar y eliminar (WHERE/DELETE)");
        String whereValue = getWhereValue(schemaName, tableName, whereColumn);
        String whereType = getColumnType(schemaName, tableName, whereColumn);
        if (whereValue == null || whereType == null) {
            return;
        }
        Record whereRecord = new Record();
        whereRecord.addColumnValue(whereColumn, whereValue, whereType);
        deleteRecord(schemaName, tableName, whereRecord);
    }

    private String getWhereValue(String schemaName, String tableName, String columnName) {
        List<String> valuesNames = getValuesNames(schemaName, tableName, columnName);
        if (valuesNames == null || valuesNames.isEmpty()) {
            System.out.println("No se pudieron obtener los valores para la columna " + columnName + ".");
            return null;
        }

        ConsoleUtils.printNumberedTable("Valores para " + columnName, valuesNames);
        int valueIndex = getValueIndex(columnName, valuesNames);
        return valuesNames.get(valueIndex);
    }

    private int getValueIndex(String columnName, List<String> valuesNames) {
        int valueIndex;
        do {
            valueIndex = ConsoleUtils.getNonNegativeIntInput("Ingrese el valor de la columna " + columnName + " para filtrar: ") - 1;
            if (valueIndex < 0 || valueIndex >= valuesNames.size()) {
                System.out.println("Índice de valor no válido. Por favor, ingrese un valor válido.");
            }
        } while (valueIndex < 0 || valueIndex >= valuesNames.size());
        return valueIndex;
    }

    private String getColumnName(String tableName, List<String> columnNames, String action) {
        ConsoleUtils.printNumberedTable("Columnas de la tabla " + tableName, columnNames);
        int columnIndex;
        do {
            columnIndex = ConsoleUtils.getNonNegativeIntInput("Seleccione una columna para " + action + ": ") - 1;
            if (columnIndex < 0 || columnIndex >= columnNames.size()) {
                System.out.println("Índice de columna no válido. Por favor, ingrese un valor válido.");
            }
        } while (columnIndex < 0 || columnIndex >= columnNames.size());
        return columnNames.get(columnIndex);
    }

    private void handleAddNewRecord(String schemaName, String tableName, List<String> columnNames) {
        if (columnNames == null || columnNames.isEmpty()) {
            System.out.println("No hay columnas disponibles para la tabla " + tableName + ".");
            return;
        }
        Record record = insertValuesOnRecord(schemaName, tableName, columnNames);
        insertNewRecord(schemaName, tableName, record);
    }

    private Record insertValuesOnRecord(String schemaName, String tableName, List<String> columnNames) {
        Record record = new Record();

        for (String columnName : columnNames) {
            String columnType = getColumnType(schemaName, tableName, columnName);
            String value = ConsoleUtils.getStringInput("Ingrese el valor para la columna " + columnName + " (" + columnType + "): ");
            record.addColumnValue(columnName, value, columnType);
        }
        return record;
    }

    private void updateRecord(String schemaName, String tableName, String columnNameNewValue, String newValue, Record whereRecord) {
        try {
            database.updateRecord(schemaName, tableName, columnNameNewValue, newValue, whereRecord);
        } catch (DatabaseException e) {
            System.out.println(e.getMessage());
        }
    }

    private void deleteRecord(String schemaName, String tableName, Record record) {
        try {
            database.deleteRecord(schemaName, tableName, record);
        } catch (DatabaseException e) {
            System.out.println(e.getMessage());
        }
    }

    private void insertNewRecord(String schemaName, String tableName, Record record) {
        try {
            database.insertRecord(schemaName, tableName, record);
        } catch (DatabaseException e) {
            System.out.println(e.getMessage());
        }
    }

    private List<String> getColumnNames(String schemaName, String tableSelected) {
        try {
            return database.getColumnsNames(schemaName, tableSelected);
        } catch (DatabaseException e) {
            System.out.println(e.getMessage());
            return null;
        }
    }

    private List<Record> getAllRecords(String schemaName, String tableName) {
        try {
            return database.selectAll(schemaName, tableName);
        } catch (DatabaseException e) {
            System.out.println(e.getMessage());
            return null;
        }
    }

    private String getColumnType(String schemaName, String tableName, String columnName){
        try {
            return database.getColumnType(schemaName, tableName, columnName);
        } catch (DatabaseException e) {
            System.out.println(e.getMessage());
            return null;
        }
    }

    private List<String> getValuesNames(String schemaName, String tableName, String columnName){
        try {
            return database.getColumnValues(schemaName, tableName, columnName);
        } catch (DatabaseException e) {
            return null;
        }
    }

    private List<String> getSchemaNames() {
        try {
            return database.getSchemas();
        } catch (DatabaseException e) {
            System.out.println(e.getMessage());
            return null;
        }
    }

    private List<String> getTablesNames(String schemaName) {
        try {
            return database.getTables(schemaName);
        } catch (DatabaseException e) {
            System.out.println(e.getMessage());
            return null;
        }
    }

    private int askTableIndex(List<String> tableNames) {
        if (tableNames == null || tableNames.isEmpty()) {
            System.out.println("No hay tablas disponibles.");
            return -1;
        }
        int tableIndex;
        do {
            tableIndex = ConsoleUtils.getNonNegativeIntInput("Seleccione una tabla: ") - 1;
            if (tableIndex < 0 || tableIndex >= tableNames.size()) {
                System.out.println("Índice de tabla no válido. Por favor, ingrese un valor válido.");
            }
        } while (tableIndex < 0 || tableIndex >= tableNames.size());
        return tableIndex;
    }

    private int askSchemaIndex(List<String> schemaNames) {
        if (schemaNames == null || schemaNames.isEmpty()) {
            System.out.println("No hay esquemas disponibles.");
            return -1;
        }
        int tableIndex;
        do {
            tableIndex = ConsoleUtils.getNonNegativeIntInput("Seleccione un esquema: ") - 1;
            if (tableIndex < 0 || tableIndex >= schemaNames.size()) {
                System.out.println("Índice de esquema no válido. Por favor, ingrese un valor válido.");
            }
        } while (tableIndex < 0 || tableIndex >= schemaNames.size());
        return tableIndex;
    }

    private int showTableOptionsMenu() {
        List<String> options = Arrays.asList(
                "Ver todos los registros",
                "Modificar un registro",
                "Eliminar un registro",
                "Agregar un registro",
                "Salir"
        );
        ConsoleUtils.printNumberedTable("Opciones disponibles", options);
        return ConsoleUtils.getNonNegativeIntInput("Seleccione una opción: ");
    }
}