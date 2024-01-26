package com.mangud.DDLCreators;

import com.mangud.Handlers.AS400Handler;
import com.mangud.Handlers.DB2Handler;
import com.mangud.Metadata.ColumnMetaData;
import com.mangud.Metadata.IndexMetaData;
import com.mangud.Metadata.TableMetaData;
import com.mangud.States.MetadataToolState;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class OracleDDL implements DDLHandler{

    @Override
    public void CreateDDLFile(MetadataToolState state, List<TableMetaData> schema, FileWriter ddlWriter) {
        if (state.getDbDDLType().equals(state.getDbType())) throw new UnsupportedOperationException("Unsupported combination.");

        switch (state.getDbType()) {
            case MYSQL, SQL -> throw new UnsupportedOperationException("Not implemented yet.");
            case AS400 -> FromAS400ToOracleDDL(state, schema, ddlWriter);
            case DB2 -> FromDB2ToOracleDDL(state, schema, ddlWriter);
            default -> throw new UnsupportedOperationException("Unsupported database.");
        };
    }

    private void FromDB2ToOracleDDL(MetadataToolState state, List<TableMetaData> schema, FileWriter ddlWriter) {
        try {
            for (TableMetaData table : schema) {
                ddlWriter.write(String.format("CREATE TABLE \"" + state.getSchema() + "\".\"" + state.getTableStart() + "%s\" (%n", table.getTableName()));
                for (Map.Entry<String, ColumnMetaData> column : table.getColumnList().entrySet()) {
                    String columnRow = getDDLRowFromDB2(column.getValue().getColumnName(), column.getValue().getDataType(), column.getValue().getLength(),
                            column.getValue().getScale(), column.getValue().isNotNull());
                    ddlWriter.write(String.format("%s,%n", columnRow));
                }
                if (!state.getAddedColumnName().isEmpty()) { ddlWriter.write(String.format("%s%n", "\"" + state.getAddedColumnName() + "\" NUMBER(22,0)")); }
                ddlWriter.write(String.format("%s%n", ")"));
                if (!state.getAddedColumnName().isEmpty() && !state.getTableSpace().isEmpty()) {
                    ddlWriter.write(String.format("%s%n", " partition by range (" + state.getAddedColumnName() + ") interval (1)"));
                    ddlWriter.write(String.format("%s%n", "(partition p000000001  values less than (2))"));
                    ddlWriter.write(String.format("%s%n%n", "tablespace " + state.getTableSpace() + ";"));
                }

                for (Map.Entry<String, IndexMetaData> index : table.getIndexList().entrySet()) {
                    String indexType = index.getValue().isUnique() ? "CREATE UNIQUE INDEX" : "CREATE INDEX";
                    String columns = index.getValue().getColumnList().stream().map(col -> "\"" + col + "\"").collect(Collectors.joining(", "));

                    String indexStr = String.format("%s %s.%s ON %s.%s (%s);%n", indexType, state.getSchema(), index.getKey(), state.getSchema(), state.getTableStart() + table.getTableName(), columns);

                    ddlWriter.write(indexStr);
                }

                if (!state.getAddedColumnName().isEmpty()) {
                    String newIndexStr = String.format("CREATE INDEX %s.%s_IDX_%s ON %s.%s (\"%s\");%n",
                            state.getSchema(), state.getAddedColumnName(), table.getTableName(), state.getToSchema(), state.getTableStart() + table.getTableName(), state.getAddedColumnName());
                    ddlWriter.write(newIndexStr);
                }
                ddlWriter.write("\n");

            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }


    }

    private void FromAS400ToOracleDDL(MetadataToolState state, List<TableMetaData> schema, FileWriter ddlWriter) {
        try {
            for (TableMetaData table : schema) {
                ddlWriter.write(String.format("CREATE TABLE \"" + state.getSchema() + "\".\"" + state.getTableStart() + "%s\" (%n", table.getTableName()));
                for (Map.Entry<String, ColumnMetaData> column : table.getColumnList().entrySet()) {
                    String columnRow = getDDLRowFromAS400(column.getValue().getColumnName(), column.getValue().getDataType(), column.getValue().getLength(),
                            column.getValue().getScale(), column.getValue().isNotNull());
                    ddlWriter.write(String.format("%s,%n", columnRow));
                }
                if (!state.getAddedColumnName().isEmpty()) { ddlWriter.write(String.format("%s%n", "\"" + state.getAddedColumnName() + "\" NUMBER(22,0)")); }
                ddlWriter.write(String.format("%s%n", ")"));
                if (!state.getAddedColumnName().isEmpty() && !state.getTableSpace().isEmpty()) {
                    ddlWriter.write(String.format("%s%n", " partition by range (" + state.getAddedColumnName() + ") interval (1)"));
                    ddlWriter.write(String.format("%s%n", "(partition p000000001  values less than (2))"));
                    ddlWriter.write(String.format("%s%n%n", "tablespace " + state.getTableSpace() + ";"));
                }

                for (Map.Entry<String, IndexMetaData> index : table.getIndexList().entrySet()) {
                    String indexType = index.getValue().isUnique() ? "CREATE UNIQUE INDEX" : "CREATE INDEX";
                    String columns = index.getValue().getColumnList().stream().map(col -> "\"" + col + "\"").collect(Collectors.joining(", "));

                    String indexStr = String.format("%s %s.%s ON %s.%s (%s);%n", indexType, state.getSchema(), index.getKey(), state.getSchema(), state.getTableStart() + table.getTableName(), columns);

                    ddlWriter.write(indexStr);
                }

                if (!state.getAddedColumnName().isEmpty()) {
                    String newIndexStr = String.format("CREATE INDEX %s.%s_IDX_%s ON %s.%s (\"%s\");%n",
                            state.getSchema(), state.getAddedColumnName(), table.getTableName(), state.getToSchema(), state.getTableStart() + table.getTableName(), state.getAddedColumnName());
                    ddlWriter.write(newIndexStr);
                }
                ddlWriter.write("\n");

            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static String getDDLRowFromDB2(String columnName, String dataType, int length, int scale, boolean notNull) {
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

    private static String getDDLRowFromAS400(String columnName, String dataType, int length, int scale, boolean notNull) {
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

}
