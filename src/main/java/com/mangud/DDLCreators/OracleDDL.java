package com.mangud.DDLCreators;

import com.mangud.Metadata.ColumnMetaData;
import com.mangud.Metadata.IndexMetaData;
import com.mangud.Metadata.TableMetaData;
import com.mangud.States.MetadataToolState;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class OracleDDL implements DDLHandler{

    @Override
    public void createDDLFile(MetadataToolState state, List<TableMetaData> schema, FileWriter ddlWriter) {


        if (state.getDbDDLType().equals(state.getDbType())) throw new UnsupportedOperationException("Unsupported combination.");

        createDLLFile(state, schema, ddlWriter);
    }

    private void createDLLFile(MetadataToolState state, List<TableMetaData> schema, FileWriter ddlWriter) {
        try {
            for (TableMetaData table : schema) {
                writeCreateTableStatement(state, table, ddlWriter);
                writeTableColumns(state, table, ddlWriter);
                writeTablePartitionAndTableSpace(state, ddlWriter);

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

    private void writeAddedColumnIndex(MetadataToolState state, TableMetaData table, FileWriter ddlWriter) throws IOException {
        if (!state.getAddedColumnName().isEmpty()) {
            String newIndexStr = String.format("CREATE INDEX %s.%s_IDX_%s ON %s.%s (\"%s\");%n",
                    state.getSchema(), state.getAddedColumnName(), table.getTableName(),
                    state.getSchema(), state.getTableStart() + table.getTableName(), state.getAddedColumnName());
            ddlWriter.write(newIndexStr);
        }
    }

    private void writeIndexStatement(Map.Entry<String, IndexMetaData> index, Writer ddlWriter, MetadataToolState state, TableMetaData table) throws IOException {
        String indexType = index.getValue().isUnique() ? "CREATE UNIQUE INDEX" : "CREATE INDEX";
        String columns = index.getValue().getColumnList().stream().map(col -> "\"" + col + "\"").collect(Collectors.joining(", "));

        String indexStr = String.format("%s %s.%s ON %s.%s (%s);%n", indexType, state.getSchema(), index.getKey(),
                state.getSchema(), state.getTableStart() + table.getTableName(), columns);

        ddlWriter.write(indexStr);
    }

    private void writeTablePartitionAndTableSpace(MetadataToolState state, Writer ddlWriter) throws IOException {
        if (!state.getAddedColumnName().isEmpty() && !state.getTableSpace().isEmpty()) {
            ddlWriter.write(String.format(" PARTITION BY RANGE (\"%s\") INTERVAL (1)%n", state.getAddedColumnName()));
            ddlWriter.write(String.format("(PARTITION p000000001 VALUES LESS THAN (2))%n"));
            ddlWriter.write(String.format(" TABLESPACE %s;%n%n", state.getTableSpace()));
        }
    }

    private void writeCreateTableStatement(MetadataToolState state, TableMetaData table, FileWriter ddlWriter) throws IOException {
        ddlWriter.write(String.format("CREATE TABLE \"%s\".\"%s%s\" (%n", state.getSchema(), state.getTableStart(), table.getTableName()));
    }

    private void writeTableColumns(MetadataToolState state, TableMetaData table, Writer ddlWriter) throws IOException {
        int totalColumns = table.getColumnList().size();
        int currentColumnIndex = 0;

        for (Map.Entry<String, ColumnMetaData> column : table.getColumnList().entrySet()) {
            String columnRow = "";
            switch (state.getDbType()) {
                case MYSQL -> columnRow = getDDLRowFromMySQL(column.getValue());
                case SQL -> columnRow = getDDLRowFromSQL(column.getValue());
                case AS400 -> columnRow = getDDLRowFromAS400(column.getValue());
                case DB2 -> columnRow = getDDLRowFromDB2(column.getValue());
                default -> throw new UnsupportedOperationException("Unsupported database.");
            }

            if (++currentColumnIndex == totalColumns) {
                writeLastColumnStatement(state, columnRow, ddlWriter);
            } else {
                ddlWriter.write(String.format("%s,%n", columnRow));
            }
        }
    }

    private void writeLastColumnStatement(MetadataToolState state, String columnRow, Writer ddlWriter) throws IOException {
        if (!state.getAddedColumnName().isEmpty()) {
            ddlWriter.write(String.format("%s,%n", columnRow));
            ddlWriter.write(String.format("\"%s\" NUMBER(22,0)%n", state.getAddedColumnName()));
        } else {
            ddlWriter.write(String.format("%s%n", columnRow));
        }
        ddlWriter.write(");\n");
    }

    private static String getDDLRowFromSQL(ColumnMetaData column) {
        StringBuilder sb = new StringBuilder();

        sb.append('\"').append(column.getColumnName()).append('\"').append(' ');

        switch (column.getDataType().toUpperCase()) {
            case "INT":
                sb.append("NUMBER(10)");
                break;
            case "SMALLINT":
                sb.append("NUMBER(5)");
                break;
            case "BIGINT":
                sb.append("NUMBER(19)");
                break;
            case "VARCHAR":
                sb.append("VARCHAR2(").append(column.getLength()).append(")");
                break;
            case "NVARCHAR":
                sb.append("NVARCHAR2(").append(column.getLength()).append(")");
                break;
            case "CHAR":
                sb.append("VARCHAR2(").append(column.getLength()).append(")");
                break;
            case "NCHAR":
                sb.append("NVARCHAR2(").append(column.getLength()).append(")");
                break;
            case "BINARY":
            case "VARBINARY":
                sb.append("RAW(").append(column.getLength()).append(")");
                break;
            case "BIT":
                sb.append("NUMBER(1)");
                break;
            case "DECIMAL":
                sb.append("NUMBER(").append(column.getLength()).append(",").append(column.getScale()).append(")");
                break;
            case "NUMERIC":
                sb.append("NUMBER(").append(column.getLength()).append(",").append(column.getScale()).append(")");
                break;
            case "FLOAT":
                sb.append("BINARY_FLOAT");
                break;
            case "REAL":
                sb.append("BINARY_DOUBLE");
                break;
            case "DATETIME":
            case "SMALLDATETIME":
                sb.append("TIMESTAMP");
                break;
            case "DATE":
                sb.append("DATE");
                break;
            case "TEXT":
                sb.append("CLOB");
                break;
            case "NTEXT":
            case "XML":
                sb.append("NCLOB");
                break;
            case "IMAGE":
                sb.append("BLOB");
                break;
            // Add more data types as needed
            default:
                sb.append("NOT FOUND/DEFAULT ").append(column.getDataType());
                break;
        }

        if (column.isNotNull()) {
            sb.append(" NOT NULL");
        }

        return sb.toString();
    }

    private static String getDDLRowFromDB2(ColumnMetaData column) {
        StringBuilder sb = new StringBuilder();

        sb.append('\"').append(column.getColumnName()).append('\"').append(' ');

        switch (column.getDataType().toUpperCase()) {
            case "DECIMAL":
            case "NUMERIC":
                sb.append("NUMBER(").append(column.getLength()).append(",").append(column.getScale()).append(")");
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
                sb.append("VARCHAR2(").append(column.getLength()).append(")");
                break;
            case "CHAR":
                sb.append("VARCHAR2(").append(column.getLength()).append(")");
                break;
            case "GRAPHIC":
                sb.append("NVARCHAR2(").append(column.getLength()).append(")");
                break;
            case "BINARY":
                sb.append("RAW(").append(column.getLength()).append(")");
                break;
            case "VARBINARY":
                sb.append("RAW(").append(column.getLength()).append(")");
                break;
            case "CHAR () FOR BIT DATA":
            case "VARCHAR () FOR BIT DATA":
                sb.append("RAW(").append(column.getLength()).append(")");
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
                sb.append("NOT FOUND/DEFAULT ").append(column.getDataType());
                break;
        }

        if (column.isNotNull()) {
            sb.append(" NOT NULL");
        }

        return sb.toString();
    }

    private static String getDDLRowFromMySQL(ColumnMetaData column) {
        StringBuilder sb = new StringBuilder();

        sb.append('\"').append(column.getColumnName()).append('\"').append(' ');

        switch (column.getDataType().toUpperCase()) {
            case "INT":
            case "MEDIUMINT":
            case "SMALLINT":
            case "TINYINT":
                sb.append("NUMBER(10)");
                break;
            case "BIGINT":
                sb.append("NUMBER(19)");
                break;
            case "VARCHAR":
                sb.append("VARCHAR2(").append(column.getLength()).append(")");
                break;
            case "CHAR":
                sb.append("VARCHAR2(").append(column.getLength()).append(")");
                break;
            case "DECIMAL":
            case "NUMERIC":
                sb.append("NUMBER(").append(column.getLength()).append(",").append(column.getScale()).append(")");
                break;
            case "BINARY":
            case "VARBINARY":
                sb.append("RAW(").append(column.getLength()).append(")");
                break;
            case "BLOB":
            case "TINYBLOB":
            case "MEDIUMBLOB":
            case "LONGBLOB":
                sb.append("BLOB");
                break;
            case "TEXT":
            case "TINYTEXT":
            case "MEDIUMTEXT":
            case "LONGTEXT":
                sb.append("CLOB");
                break;
            case "DATE":
                sb.append("DATE");
                break;
            case "TIME":
                sb.append("INTERVAL DAY(0) TO SECOND");
                break;
            case "DATETIME":
            case "TIMESTAMP":
                sb.append("TIMESTAMP");
                break;
            case "YEAR":
                sb.append("NUMBER(4)");
                break;
            case "FLOAT":
                sb.append("BINARY_FLOAT");
                break;
            case "DOUBLE":
                sb.append("BINARY_DOUBLE");
                break;
            // Add more data types as needed
            default:
                sb.append("NOT FOUND/DEFAULT ").append(column.getDataType());
                break;
        }

        if (column.isNotNull()) {
            sb.append(" NOT NULL");
        }

        return sb.toString();
    }


    private static String getDDLRowFromAS400(ColumnMetaData column) {
        StringBuilder sb = new StringBuilder();

        sb.append('\"').append(column.getColumnName()).append('\"').append(' ');

        switch (column.getDataType().toUpperCase()) {
            case "DECIMAL":
            case "NUMERIC":
                sb.append("NUMBER(").append(column.getLength()).append(",").append(column.getScale()).append(")");
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
                sb.append("VARCHAR2(").append(column.getLength()).append(")");
                break;
            case "CHAR":
                sb.append("VARCHAR2(").append(column.getLength()).append(")");
                break;
            case "GRAPHIC":
                sb.append("NVARCHAR2(").append(column.getLength()).append(")");
                break;
            case "BINARY":
                sb.append("RAW(").append(column.getLength()).append(")");
                break;
            case "VARBINARY":
                sb.append("RAW(").append(column.getLength()).append(")");
                break;
            case "CHAR () FOR BIT DATA":
            case "VARCHAR () FOR BIT DATA":
                sb.append("RAW(").append(column.getLength()).append(")");
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
                sb.append("NOT FOUND/DEFAULT ").append(column.getDataType());
                break;
        }

        if (column.isNotNull()) {
            sb.append(" NOT NULL");
        }

        return sb.toString();
    }

}
