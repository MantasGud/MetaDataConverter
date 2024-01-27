package com.mangud.Handlers;

import com.mangud.Metadata.ColumnMetaData;
import com.mangud.Metadata.IndexMetaData;
import com.mangud.Metadata.TableMetaData;
import com.mangud.States.MetadataToolState;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

public class DB2Handler implements DatabaseHandler{
    @Override
    public TableMetaData extractTableMetadata(DatabaseMetaData metaData, String tableName, MetadataToolState state) {
        Map<String, ColumnMetaData> columnData = ExtractColumnsFromTable(metaData, tableName, state);
        Map<String, IndexMetaData> indexData = ExtractIndexFromTable(metaData, tableName, state);
        return new TableMetaData(tableName, columnData, indexData);
    }


    private Map<String, ColumnMetaData> ExtractColumnsFromTable(DatabaseMetaData metaData, String tableName, MetadataToolState state) {
        Map<String, ColumnMetaData> columnMap = new LinkedHashMap<>();
        try (ResultSet columnRs = metaData.getColumns(null, state.getSchema(), tableName, null)) {
            while (columnRs.next()) {
                ColumnMetaData columnMetadata = new ColumnMetaData(
                        columnRs.getString("COLUMN_NAME"),
                        columnRs.getString("TYPE_NAME"),
                        columnRs.getInt("COLUMN_SIZE"),
                        columnRs.getInt("DECIMAL_DIGITS"),
                        columnRs.getInt("NULLABLE") == DatabaseMetaData.columnNoNulls,
                        columnRs.getBoolean("IS_AUTOINCREMENT"),
                        columnRs.getString("REMARKS")
                );
                columnMap.put(columnRs.getString("COLUMN_NAME"), columnMetadata);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return columnMap;
    }

    private Map<String, IndexMetaData> ExtractIndexFromTable(DatabaseMetaData metaData, String tableName, MetadataToolState state) {
        Map<String, IndexMetaData> indexMap = new LinkedHashMap<>();
        try (ResultSet indexRs = metaData.getIndexInfo(null, state.getSchema(), tableName, false, false)) {
            Map<String, List<String>> indexColumnsMap = new HashMap<>();
            Map<String, Boolean> uniqueIndexMap = new HashMap<>();

            Set<String> tableColumnNames = new HashSet<>();
            try (ResultSet columnsRs = metaData.getColumns(null, state.getSchema(), tableName, "%")) {
                while (columnsRs.next()) {
                    tableColumnNames.add(columnsRs.getString("COLUMN_NAME"));
                }
            }

            Set<String> uniqueColumnSets = new HashSet<>();

            while (indexRs.next()) {
                String indexName = indexRs.getString("INDEX_NAME");
                if (indexName == null) {
                    continue;
                }

                boolean nonUnique = indexRs.getBoolean("NON_UNIQUE");
                String columnName = indexRs.getString("COLUMN_NAME");

                if (indexColumnsMap.containsKey(indexName)) {
                    indexColumnsMap.get(indexName).add(columnName);
                } else {
                    List<String> columnList = new ArrayList<>();
                    columnList.add(columnName);
                    indexColumnsMap.put(indexName, columnList);
                    uniqueIndexMap.put(indexName, !nonUnique);
                }
            }

            // Removes indexes that have columns not in the table and prints a message for each deleted index
            indexColumnsMap.entrySet().removeIf(entry -> {
                boolean shouldRemove = entry.getValue().stream().anyMatch(column -> !tableColumnNames.contains(column));
                if (shouldRemove) {
                    System.out.println("Table : " + tableName + " .Removing index '" + entry.getKey() + "' as it contains columns not in the table.");
                }
                return shouldRemove;
            });

            for (Map.Entry<String, List<String>> entry : indexColumnsMap.entrySet()) {
                String indexName = entry.getKey();
                List<String> columnList = entry.getValue();
                if (!state.getAddedColumnName().isEmpty()) { columnList.add(state.getAddedColumnName()); }

                String sortedColumns = columnList.stream().sorted().collect(Collectors.joining(","));
                if (uniqueColumnSets.contains(sortedColumns)) {
                    System.out.println("Table : " + tableName + " .Skipping index '" + indexName + "' as it has duplicate columns.");
                    continue; // Skip if the column combination already exists
                } else {
                    uniqueColumnSets.add(sortedColumns);
                }

                boolean unique = uniqueIndexMap.get(indexName);
                IndexMetaData indexMetadata = new IndexMetaData(indexName, unique, columnList);
                indexMap.put(indexName, indexMetadata);

            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return indexMap;
    }
}
