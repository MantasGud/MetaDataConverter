package com.mangud.Handlers;

import com.mangud.Metadata.ColumnMetaData;
import com.mangud.States.MetadataToolState;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

public class AS400ToOracleHandler implements DatabaseHandler{

    public void processTable(DatabaseMetaData metaData, String tableName, FileWriter writer, FileWriter ddlWriter, MetadataToolState state) throws SQLException, IOException {
        writeTableMetadata(metaData, tableName, writer, ddlWriter, state);
        writeIndexMetadata(metaData, tableName, writer, ddlWriter, state);
    }

    private static void writeTableMetadata(DatabaseMetaData metaData, String tableName, FileWriter writer, FileWriter ddlWriter, MetadataToolState state) throws SQLException, IOException {
        ddlWriter.write(String.format("CREATE TABLE \"" + state.getToSchema() + "\".\"" + state.getTableStart() + "%s\" (%n", tableName));

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

                String ddl = getDDLForOracle(columnMetadata.getColumnName(), columnMetadata.getDataType(),
                        columnMetadata.getLength(), columnMetadata.getScale(), columnMetadata.isNotNull());
                writer.write(String.format("%s,%s,%s,%d,%d,%s,%s,%s,%n",
                        tableName, columnMetadata.getColumnName(), columnMetadata.getDataType(), columnMetadata.getLength(),
                        columnMetadata.getScale(),
                        columnMetadata.isNotNull() ? "Yes" : "No",
                        columnMetadata.isAutoIncrement() ? "Yes" : "No",
                        columnMetadata.getDescription()));
                ddlWriter.write(String.format("%s,%n", ddl));
            }
        }

        if (!state.getAddedColumnName().isEmpty()) { ddlWriter.write(String.format("%s%n", "\"" + state.getAddedColumnName() + "\" NUMBER(22,0)")); }

        ddlWriter.write(String.format("%s%n", ")"));

        if (!state.getAddedColumnName().isEmpty()) {
            ddlWriter.write(String.format("%s%n", " partition by range (" + state.getAddedColumnName() + ") interval (1)"));
            ddlWriter.write(String.format("%s%n", "(partition p000000001  values less than (2))"));
            ddlWriter.write(String.format("%s%n%n", "tablespace " + state.getTableSpace() + ";"));
        }

    }

    private static void writeIndexMetadata(DatabaseMetaData metaData, String tableName, FileWriter writer, FileWriter ddlWriter, MetadataToolState state) throws SQLException, IOException {
        try (ResultSet indexRs = metaData.getIndexInfo(null, state.getSchema(), tableName, false, false)) {
            Map<String, List<String>> indexColumnsMap = new HashMap<>();
            Map<String, Boolean> uniqueIndexMap = new HashMap<>();
            String tableFullName = state.getTableStart() + tableName;

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
                    System.out.println("Table : " + tableName + " .Skipping index '" + indexName + "' as it has duplicate columns."); // Added print statement
                    continue; // Skip if the column combination already exists
                } else {
                    uniqueColumnSets.add(sortedColumns);
                }

                boolean unique = uniqueIndexMap.get(indexName);

                String indexType = unique ? "CREATE UNIQUE INDEX" : "CREATE INDEX";
                String columns = columnList.stream().map(col -> "\"" + col + "\"").collect(Collectors.joining(", "));

                String indexStr = String.format("%s %s.%s ON %s.%s (%s);%n", indexType, state.getToSchema(), indexName, state.getToSchema(), tableFullName, columns);

                writer.write(indexStr);
                ddlWriter.write(indexStr);
            }

            if (!state.getAddedColumnName().isEmpty()) {
                String newIndexStr = String.format("CREATE INDEX %s.%s_IDX_%s ON %s.%s (\"%s\");%n",
                        state.getToSchema(), state.getAddedColumnName(), tableName, state.getToSchema(), tableFullName, state.getAddedColumnName());
                writer.write(newIndexStr);
                ddlWriter.write(newIndexStr);
            }
            writer.write("\n");
            ddlWriter.write("\n");

        }
    }

    private static String getDDLForOracle(String columnName, String dataType, int length, int scale, boolean notNull) {
        StringBuilder sb = new StringBuilder();

        sb.append('\"').append(columnName).append('\"').append(' ');

        switch (dataType) {
            case "DECIMAL":
            case "NUMERIC":
                sb.append("NUMBER(").append(length).append(",").append(scale).append(")");
                break;
            case "INTEGER":
                sb.append("NUMBER");
                break;
            case "SMALLINT":
                sb.append("NUMBER(5)");
                break;
            case "BIGINT":
                sb.append("NUMBER(19)");
                break;
            case "VARCHAR":
                sb.append("VARCHAR2(").append(length).append(")");
                break;
            case "CHAR":
                sb.append("VARCHAR2(").append(length).append(")");
                break;
            case "GRAPHIC":
                sb.append("NVARCHAR2(").append(length).append(")");
                break;
            case "BINARY":
                sb.append("RAW(").append(length).append(")");
                break;
            case "VARBINARY":
                sb.append("RAW(").append(length).append(")");
                break;
            case "CHAR () FOR BIT DATA":
            case "VARCHAR () FOR BIT DATA":
                sb.append("RAW(").append(length).append(")");
                break;
            case "CLOB":
                sb.append("CLOB");
                break;
            case "BLOB":
                sb.append("BLOB");
                break;
            case "ROWID":
                sb.append("UROWID");
                break;
            case "REAL":
                sb.append("BINARY_FLOAT");
                break;
            case "DATE":
            case "TIMESTAMP":
                sb.append("DATE");
                break;
            case "TIME":
                sb.append("INTERVAL DAY(0) TO SECOND");
                break;
            case "TIMESTAMP WITH TIME ZONE":
                sb.append("TIMESTAMP WITH TIME ZONE");
                break;
            case "DOUBLE":
                sb.append("BINARY_DOUBLE");
                break;
            case "XML":
                sb.append("XMLTYPE");
                break;
            // Add more data types as needed
            default:
                sb.append("NOT FOUND/DEFAULT ").append(dataType);
                break;
        }

        if (notNull) {
            sb.append(" NOT NULL");
        }

        return sb.toString();
    }

    @Override
    public void extractTableMetadata(DatabaseMetaData metaData, String tableName, MetadataToolState state) {

    }
}
