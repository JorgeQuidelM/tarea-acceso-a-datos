package org.example;

import java.util.ArrayList;
import java.util.List;

public class Record {
    private List<String> columnNames = new ArrayList<>();
    private List<String> columnValues = new ArrayList<>();

    public void addColumnValue(String columnName, String value) {
        columnNames.add(columnName);
        columnValues.add(value);
    }

    public String getValue(String columnName) {
        int index = columnNames.indexOf(columnName);
        if (index != -1) {
            return columnValues.get(index);
        }
        return null; // O maneja el caso donde la columna no existe
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    @Override
    public String toString() {
        StringBuilder resultBuilder = new StringBuilder();
        for (int i = 0; i < columnNames.size(); i++) {
            resultBuilder.append(columnNames.get(i)).append(": ").append(columnValues.get(i)).append(", ");
        }
        String result = resultBuilder.toString();
        return result.substring(0, result.length() - 2);
    }
}
