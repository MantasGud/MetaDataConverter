package com.ManGud;

import java.io.*;
import java.net.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Main
{
    private static final String schemaFullName = "Test_SCH";
    private static final String addedColumnName = "";
    private static final String tableStart = "";

    public static void main(String[] args) {
        try {
            processArguments(args);
        } catch (ClassNotFoundException | MalformedURLException | UnknownHostException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static void processArguments(String[] args) throws ClassNotFoundException, MalformedURLException, UnknownHostException {
        if (args.length != 7) {
            System.err.println("Usage: DB2 as400 <url> <username> <password> <schema> <output file name> <table list file> <as400 file jar>");
            System.exit(1);
        }

        String schema = args[3];
        String url = "jdbc:as400://" + args[0] + ";libraries=" + schema + ";";
        String user = args[1];
        String password = args[2];
        String fileName = args[4];
        String ddlFileName = args[4] + "_ddl.sql";
        String tableListFile = args[5];
        String as400Location = args[6];


        configureClasspath(as400Location);
        List<String> tableNames = createTableList(tableListFile);
        createMetadataFile(url, user, password, schema, fileName, ddlFileName, tableNames);
    }

    private static List<String> createTableList(String tableListFile) {
        List<String> tableNames = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(tableListFile))) {
            String line;
            while ((line = br.readLine()) != null) {
                tableNames.add(line.trim());
            }
        } catch (IOException e) {
            System.err.println("Failed to read table list file");
            e.printStackTrace();
            System.exit(1);
        }
        return tableNames;
    }

    private static void configureClasspath(String as400Location) throws MalformedURLException, ClassNotFoundException {
        URL[] urls = {new File(as400Location).toURI().toURL()};
        URLClassLoader loader = new URLClassLoader(urls, Thread.currentThread().getContextClassLoader());
        Thread.currentThread().setContextClassLoader(loader);
        Class.forName("com.ibm.as400.access.AS400JDBCDriver", true, loader);
    }

    private static void createMetadataFile(String url, String user, String password, String schema, String fileName, String ddlFileName, List<String> tableNames) throws UnknownHostException {
        try (Connection conn = DriverManager.getConnection(url, user, password)) {
            DatabaseMetaData metaData = conn.getMetaData();

            try (FileWriter writer = new FileWriter(fileName + ".csv");
                 FileWriter ddlWriter = new FileWriter(ddlFileName)) {
                writeMetadataToCsvFile(metaData, schema, writer, ddlWriter, tableNames);
            } catch (IOException e) {
                System.err.println("Failed to write metadata to CSV file");
                e.printStackTrace();
            }
        } catch (SQLException e) {
            System.err.println("Failed to connect to database");
            InetAddress localAddress = InetAddress.getLocalHost();
            System.err.println("Address - " + localAddress);
            System.err.println("Url - " + url);
            e.printStackTrace();
        }
    }

    private static void writeMetadataToCsvFile(DatabaseMetaData metaData, String schema, FileWriter writer, FileWriter ddlWriter, List<String> tableNames) throws SQLException, IOException {
        writer.write("Table Name, Column Name, Data Type, Length, Scale, Not Null, Auto Increment\n");

        //scanWholeSchema(metaData, schema, writer, ddlWriter);
        scanTablesFromList(metaData, schema, writer, ddlWriter, tableNames);

    }

    private static void scanTablesFromList(DatabaseMetaData metaData, String schema, FileWriter writer, FileWriter ddlWriter, List<String> tableNames) throws SQLException, IOException {
        for (String tableName : tableNames) {
            writeTableMetadata(metaData, schema, tableName, writer, ddlWriter);
            writeIndexMetadata(metaData, schema, tableName, writer, ddlWriter);
        }
    }

    private static void scanWholeSchema(DatabaseMetaData metaData, String schema, FileWriter writer, FileWriter ddlWriter) throws SQLException, IOException {
        try (ResultSet tableRs = metaData.getTables(null, schema, null, new String[]{"TABLE"})) {
            while (tableRs.next()) {
                String tableName = tableRs.getString("TABLE_NAME");
                writeTableMetadata(metaData, schema, tableName, writer, ddlWriter);
                writeIndexMetadata(metaData, schema, tableName, writer, ddlWriter);
            }
        }
    }

    private static void writeTableMetadata(DatabaseMetaData metaData, String schema, String tableName, FileWriter writer, FileWriter ddlWriter) throws SQLException, IOException {
        ddlWriter.write(String.format("CREATE TABLE \"" + schemaFullName + "\".\"" + tableStart + "%s\" (%n", tableName));

        try (ResultSet columnRs = metaData.getColumns(null, schema, tableName, null)) {
            while (columnRs.next()) {
                String columnName = columnRs.getString("COLUMN_NAME");
                String dataType = columnRs.getString("TYPE_NAME");
                int length = columnRs.getInt("COLUMN_SIZE");
                int scale = columnRs.getInt("DECIMAL_DIGITS");
                boolean notNull = (columnRs.getInt("NULLABLE") == DatabaseMetaData.columnNoNulls);
                boolean autoIncrement = columnRs.getBoolean("IS_AUTOINCREMENT");
                String description = columnRs.getString("REMARKS");

                String ddl = getDDLForOracle(columnName, dataType, length, scale, notNull);
                writer.write(String.format("%s,%s,%s,%d,%d,%s,%s,%s,%n",
                        tableName, columnName, dataType, length, scale,
                        notNull ? "Yes" : "No",
                        autoIncrement ? "Yes" : "No",
                        description));
                ddlWriter.write(String.format("%s,%n", ddl));
            }
        }

        if (!addedColumnName.equals("")) { ddlWriter.write(String.format("%s%n", "\"" + addedColumnName + "\" NUMBER(22,0)")); }

        ddlWriter.write(String.format("%s%n", ")"));

        if (!addedColumnName.equals("")) {
            ddlWriter.write(String.format("%s%n", " partition by range (" + addedColumnName + ") interval (1)"));
            ddlWriter.write(String.format("%s%n", "(partition p000000001  values less than (2))"));
            ddlWriter.write(String.format("%s%n%n", "tablespace TDP_DATA01;"));
        }

    }

    private static void writeIndexMetadata(DatabaseMetaData metaData, String schema, String tableName, FileWriter writer, FileWriter ddlWriter) throws SQLException, IOException {
        try (ResultSet indexRs = metaData.getIndexInfo(null, schema, tableName, false, false)) {
            Map<String, List<String>> indexColumnsMap = new HashMap<>();
            Map<String, Boolean> uniqueIndexMap = new HashMap<>();
            String tableFullName = tableStart + tableName;

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

            for (Map.Entry<String, List<String>> entry : indexColumnsMap.entrySet()) {
                String indexName = entry.getKey();
                List<String> columnList = entry.getValue();
                if (!addedColumnName.equals("")) { columnList.add(addedColumnName); }

                boolean unique = uniqueIndexMap.get(indexName);

                String indexType = unique ? "CREATE UNIQUE INDEX" : "CREATE INDEX";
                String columns = columnList.stream().map(col -> "\"" + col + "\"").collect(Collectors.joining(", "));

                String indexStr = String.format("%s %s.%s ON %s.%s (%s);%n", indexType, schemaFullName, indexName, schemaFullName, tableFullName, columns);

                writer.write(indexStr);
                ddlWriter.write(indexStr);
            }

            if (!addedColumnName.equals("")) {
                // Create new index with the "TDO_ID" column for every table
                String newIndexStr = String.format("CREATE INDEX %s.%s_IDX_%s ON %s.%s (\"%s\");%n",
                        addedColumnName, schemaFullName, tableName, schemaFullName, tableFullName, addedColumnName);
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
            case "VARCHAR":
                sb.append("VARCHAR2(").append(length).append(")");
                break;
            case "CHAR":
                sb.append("VARCHAR2(").append(length).append(")");
                break;
            case "CHAR () FOR BIT DATA":
                sb.append("RAW(").append(length).append(")");
                break;
            case "CLOB":
                sb.append("CLOB");
                break;
            case "DATE":
            case "TIMESTAMP":
                sb.append("DATE");
                break;
            // Add more data types as needed
            default:
                sb.append("NOT FOUND/DEFAULT " + dataType);
                break;
        }

        if (notNull) {
            sb.append(" NOT NULL ENABLE");
        }

        return sb.toString();
    }
}
