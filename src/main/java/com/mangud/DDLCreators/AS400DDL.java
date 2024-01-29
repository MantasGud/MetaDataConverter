package com.mangud.DDLCreators;

import com.mangud.Metadata.ColumnMetaData;
import com.mangud.Metadata.IndexMetaData;
import com.mangud.Metadata.TableMetaData;
import com.mangud.States.MetadataToolState;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class AS400DDL implements DDLHandler{
    @Override
    public void createDDLFile(MetadataToolState state, List<TableMetaData> schema, FileWriter ddlWriter) {

        if (state.getDbDDLType().equals(state.getDbType())) throw new UnsupportedOperationException("Unsupported combination.");

        switch (state.getDbType()) {
            case MYSQL, SQL, DB2 -> throw new UnsupportedOperationException("Not implemented yet.");
        }

        createDLLFile(state, schema, ddlWriter);
    }

    private void createDLLFile(MetadataToolState state, List<TableMetaData> schema, FileWriter ddlWriter) {
        try {
            for (TableMetaData table : schema) {
                writeCreateTableStatement(state, table, ddlWriter);
                writeTableColumns(state, table, ddlWriter);

                for (Map.Entry<String, IndexMetaData> index : table.getIndexList().entrySet()) {
                    writeIndexStatement(index, ddlWriter, state, table);
                }

                writeAddedColumnIndex(state, table, ddlWriter);
                ddlWriter.write("\n");

            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void writeCreateTableStatement(MetadataToolState state, TableMetaData table, FileWriter ddlWriter) throws IOException {
        ddlWriter.write(String.format("CREATE TABLE \"%s\".\"%s%s\" (%n", state.getSchema(), state.getTableStart(), table.getTableName()));
    }

    private void writeTableColumns(MetadataToolState state, TableMetaData table, FileWriter ddlWriter) throws IOException {
        int totalColumns = table.getColumnList().size();
        int currentColumnIndex = 0;

        for (Map.Entry<String, ColumnMetaData> column : table.getColumnList().entrySet()) {
            String columnRow;
            switch (state.getDbType()) {
                case ORACLE -> columnRow = getDDLRowFromOracle(column.getValue());
                default -> throw new UnsupportedOperationException("Unsupported database.");
            }

            if (++currentColumnIndex == totalColumns) {
                writeLastColumnStatement(state, columnRow, ddlWriter);
            } else {
                ddlWriter.write(String.format("%s,%n", columnRow));
            }
        }
    }

    private void writeLastColumnStatement(MetadataToolState state, String columnRow, FileWriter ddlWriter) throws IOException {
        if (!state.getAddedColumnName().isEmpty()) {
            ddlWriter.write(String.format("%s,%n", columnRow));
            ddlWriter.write(String.format("\"%s\" DECIMAL(22,0)%n", state.getAddedColumnName()));
        } else {
            ddlWriter.write(String.format("%s%n", columnRow));
        }
        ddlWriter.write(");\n");
    }

    private void writeIndexStatement(Map.Entry<String, IndexMetaData> index, FileWriter ddlWriter, MetadataToolState state, TableMetaData table) throws IOException {
        String indexType = index.getValue().isUnique() ? "CREATE UNIQUE INDEX" : "CREATE INDEX";
        String columns = index.getValue().getColumnList().stream().map(col -> "\"" + col + "\"").collect(Collectors.joining(", "));

        String indexStr = String.format("%s %s.%s ON %s.%s (%s);%n", indexType, state.getSchema(), index.getKey(),
                state.getSchema(), state.getTableStart() + table.getTableName(), columns);

        ddlWriter.write(indexStr);
    }

    private void writeAddedColumnIndex(MetadataToolState state, TableMetaData table, FileWriter ddlWriter) throws IOException {
        if (!state.getAddedColumnName().isEmpty()) {
            String newIndexStr = String.format("CREATE INDEX %s.%s_IDX_%s ON %s.%s (\"%s\");%n",
                    state.getSchema(), state.getAddedColumnName(), table.getTableName(),
                    state.getSchema(), state.getTableStart() + table.getTableName(), state.getAddedColumnName());
            ddlWriter.write(newIndexStr);
        }
    }

    private static String getDDLRowFromOracle(ColumnMetaData column) {
        StringBuilder sb = new StringBuilder();

        sb.append('\"').append(column.getColumnName()).append('\"').append(' ');

        switch (column.getDataType().toUpperCase()) {
            case "NUMBER":
                if (column.getScale() > 0) {
                    sb.append("DECIMAL(").append(column.getLength())
                            .append(",").append(column.getScale()).append(")");
                } else {
                    sb.append("INTEGER");
                }
                break;
            case "VARCHAR2":
            case "NVARCHAR2":
                sb.append("VARCHAR(").append(column.getLength()).append(")");
                break;
            case "CHAR":
            case "NCHAR":
                sb.append("CHAR(").append(column.getLength()).append(")");
                break;
            case "DATE":
                sb.append("DATE");
                break;
            case "TIMESTAMP(9)":
                sb.append("TIMESTAMP");
                break;
            case "TIMESTAMP":
                sb.append("TIMESTAMP");
                break;
            case "CLOB":
                sb.append("CLOB");
                break;
            case "BLOB":
                sb.append("BLOB");
                break;
            case "FLOAT":
                sb.append("FLOAT");
            case "LONG":
                sb.append("CLOB");
                break;
            case "RAW":
                sb.append("BINARY");
                break;
            case "LONG RAW":
                sb.append("BLOB");
                break;
            case "BINARY_FLOAT":
                sb.append("REAL");
                break;
            case "BINARY_DOUBLE":
                sb.append("DOUBLE");
                break;
            case "XMLTYPE":
                sb.append("XML");
                break;
            default:
                sb.append("NOT FOUND/DEFAULT ").append(column.getDataType());
                break;
        }

        if (column.isNotNull()) {
            sb.append(" NOT NULL");
        }

        return sb.toString();
    }

}
