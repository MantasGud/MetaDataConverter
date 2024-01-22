package com.mangud.Metadata;

import com.mangud.Handlers.AS400ToOracleHandler;
import com.mangud.Handlers.DatabaseHandler;
import com.mangud.States.MetadataToolState;

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.*;
import java.util.*;

public class MetaDataProcessor {

    public void processMetaData(MetadataToolState state) throws UnknownHostException {
        state.setfullUrlWithLibraries();
        if (!state.isValidInfo()) {
            System.err.println("State info is not valid.");
            return;
        }

        createMetaDataFiles(state);
    }

    public void testConnection(MetadataToolState state) throws UnknownHostException {
        state.setfullUrlWithLibraries();
        if (!state.isValidInfo()) {
            System.err.println("State info is not valid.");
            return;
        }

        tryConnectionToDatabase(state);
    }

    public DatabaseHandler getDatabaseHandler(int sourceDbType, int targetDbType) {
        if ((sourceDbType == 5 || sourceDbType == 4) & targetDbType == 1) {
            return new AS400ToOracleHandler();
        }

        throw new UnsupportedOperationException("Unsupported database combination.");
    }

    private void createMetaDataFiles(MetadataToolState state) throws UnknownHostException {
        try (Connection conn = DriverManager.getConnection(state.getFullUrlWithLibraries(), state.getUsername(), state.getPassword())) {
            DatabaseMetaData metaData = conn.getMetaData();

            try (FileWriter writer = new FileWriter(state.getOutputFile() + ".csv");
                 FileWriter ddlWriter = new FileWriter(state.getOutputFile() + "_ddl.sql")) {
                writeMetadataToFiles(metaData, writer, ddlWriter, state);
            } catch (IOException e) {
                System.err.println("Failed to write metadata to CSV file");
                e.printStackTrace();
            }
        } catch (SQLException e) {
            System.err.println("Failed to connect to database");
            InetAddress localAddress = InetAddress.getLocalHost();
            System.err.println("Address - " + localAddress);
            System.err.println("Url - " + state.getFullUrlWithLibraries());
            e.printStackTrace();
        }
    }

    private void writeMetadataToFiles(DatabaseMetaData metaData, FileWriter writer, FileWriter ddlWriter, MetadataToolState state) throws SQLException, IOException {
        writer.write("Table Name, Column Name, Data Type, Length, Scale, Not Null, Auto Increment\n");

        DatabaseHandler databaseHandler = getDatabaseHandler(state.getDbType().getDbType(), 1);
        switch (state.getTableReadType()) {
            case 0:
                scanWholeSchema(metaData, writer, ddlWriter, databaseHandler, state);
                break;
            case 1:
                scanTablesFromList(metaData, writer, ddlWriter, databaseHandler, state);
                break;
            default:
                System.out.println("Invalid choice. Please try again.");
        }

    }

    private void scanTablesFromList(DatabaseMetaData metaData, FileWriter writer, FileWriter ddlWriter, DatabaseHandler dh, MetadataToolState state) throws SQLException, IOException {
        for (String tableName : state.getTableNames()) {
            dh.processTable(metaData, tableName, writer, ddlWriter, state);
        }
    }

    private void scanWholeSchema(DatabaseMetaData metaData, FileWriter writer, FileWriter ddlWriter, DatabaseHandler dh, MetadataToolState state) throws SQLException, IOException {
        try (ResultSet tableRs = metaData.getTables(null, state.getSchema(), null, new String[]{"TABLE"})) {
            while (tableRs.next()) {
                String tableName = tableRs.getString("TABLE_NAME");
                dh.processTable(metaData, tableName, writer, ddlWriter, state);
            }
        }
    }

    private void tryConnectionToDatabase(MetadataToolState state)  throws UnknownHostException {
        try (Connection conn = DriverManager.getConnection(state.getFullUrlWithLibraries(), state.getUsername(), state.getPassword())) {
            DatabaseMetaData metaData = conn.getMetaData();
            System.out.println("Connection was successful.");
        } catch (SQLException e) {
            System.err.println("Failed to connect to database");
            InetAddress localAddress = InetAddress.getLocalHost();
            System.err.println("Address - " + localAddress);
            System.err.println("Url - " + state.getUrl());
            e.printStackTrace();
        }
    }


    public void addDDLToDatabase(String databaseUrl, String dbUser, String dbPassword, String ddlFileName) {
        try (Connection connection = DriverManager.getConnection(databaseUrl, dbUser, dbPassword);
             Statement statement = connection.createStatement()) {

            StringBuilder sb = new StringBuilder();
            try (BufferedReader br = new BufferedReader(new FileReader(ddlFileName))) {
                String line;
                while ((line = br.readLine()) != null) {
                    sb.append(line);
                }
            } catch (IOException e) {
                e.printStackTrace();
                return;
            }

            // Split the SQL statements and execute them
            String[] sqlStatements = sb.toString().split(";");

            for (String sql : sqlStatements) {
                if (!sql.trim().isEmpty()) {
                    statement.execute(sql.trim());
                }
            }

            System.out.println("Tables and index created successfully!");

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void writeToFile(List<String> diffSql, String outputFilePath) throws IOException {
        BufferedWriter bw = new BufferedWriter(new FileWriter(outputFilePath));
        for (String sql : diffSql) {
            bw.write(sql);
            bw.newLine();
        }
        bw.close();
    }

    public void compareTstToDev(MetadataToolState state) {
        //Username for database also schema name
        String dbUser = "";
        //Database password
        String dbPassword = "";
        //Dev database
        String dbUrl1 = "jdbc:oracle:thin:@//";
        //Prod database
        String dbUrl2 = "jdbc:oracle:thin:@//";
        //File name to write
        String compareFile = "SchemaCompareDLLAndOracle.txt";

        try (Connection conn1 = DriverManager.getConnection(dbUrl2, dbUser, dbPassword);
             Connection conn2 = DriverManager.getConnection(dbUrl1, dbUser, dbPassword);){

            // Compare the schema information and generate required SQL statements
            List<String> diffSql = compareSchemasTSTtoDev(conn1, conn2, dbUser);

            // Write the generated SQL statements to a file
            writeToFile(diffSql, compareFile);

            // Close the database connection
            conn1.close();
            conn2.close();
        } catch (SQLException | IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    private static List<String> compareSchemasTSTtoDev(Connection conn, Connection conn2, String schemaName) throws SQLException {
        Map<String, Set<String>> prdTables = getSchemaTablesAndIndexes(conn, schemaName);
        Map<String, Set<String>> tstTables = getSchemaTablesAndIndexes(conn2, schemaName);
        List<String> differences = new ArrayList<>();

        for (String tableName : tstTables.keySet()) {
            if (prdTables.containsKey(tableName)) {
                Set<String> prdColumns = getTableColumns(conn, schemaName, tableName);
                Set<String> tstColumns = getTableColumns(conn2, schemaName, tableName);

                differences.addAll(compareTableColumns(tableName, tstColumns, prdColumns));

                Set<String> prdIndexes = prdTables.get(tableName);
                Set<String> tstIndexes = tstTables.get(tableName);

                for (String indexName : tstIndexes) {
                    if (!prdIndexes.contains(indexName)) {
                        differences.add("Index missing in prd: " + indexName + " for table: " + tableName);
                    }
                }
            } else {
                differences.add("Table missing in prd: " + tableName + ", present in tst");
            }
        }

        // Check for tables present in prd schema but not in tst schema
        for (String tableName : prdTables.keySet()) {
            if (!tstTables.containsKey(tableName)) {
                differences.add("Table missing in tst: " + tableName + ", present in prd");
            }
        }

        return differences;
    }

    private static List<String> compareTableColumns(String tableName, Set<String> tstColumns, Set<String> prdColumns) {
        List<String> columnDifferences = new ArrayList<>();

        for (String columnName : tstColumns) {
            if (!prdColumns.contains(columnName)) {
                columnDifferences.add("Column missing in prd: " + columnName + " for table: " + tableName);
            }
        }

        return columnDifferences;
    }

    private static Set<String> getTableColumns(Connection conn, String schema, String tableName) throws SQLException {
        Set<String> columns = new HashSet<>();

        try (Statement stmt = conn.createStatement()) {
            String query = "SELECT column_name " +
                    "FROM all_tab_columns " +
                    "WHERE owner = '" + schema + "' AND table_name = '" + tableName + "' " +
                    "ORDER BY column_name";

            ResultSet rs = stmt.executeQuery(query);

            while (rs.next()) {
                String columnName = rs.getString("column_name");
                columns.add(columnName);
            }
        }

        return columns;
    }

    private static Map<String, Set<String>> getSchemaTablesAndIndexes(Connection conn, String schema) throws SQLException {
        Map<String, Set<String>> tablesAndIndexes = new HashMap<>();

        try (Statement stmt = conn.createStatement()) {
            String query = "SELECT t.table_name, i.index_name, c.column_name " +
                    "FROM all_tables t " +
                    "LEFT JOIN all_indexes i ON t.table_name = i.table_name AND t.owner = i.table_owner " +
                    "LEFT JOIN all_tab_columns c ON t.table_name = c.table_name AND t.owner = c.owner " +
                    "WHERE t.owner = '" + schema + "' " +
                    "ORDER BY t.table_name, i.index_name, c.column_name";

            ResultSet rs = stmt.executeQuery(query);

            while (rs.next()) {
                String tableName = rs.getString("table_name");
                String indexName = rs.getString("index_name");
                String columnName = rs.getString("column_name");

                tablesAndIndexes.computeIfAbsent(tableName, k -> new HashSet<>());

                if (indexName != null) {
                    tablesAndIndexes.get(tableName).add("INDEX:" + indexName);
                }
                if (columnName != null) {
                    tablesAndIndexes.get(tableName).add("COLUMN:" + columnName);
                }
            }
        }

        return tablesAndIndexes;
    }
}
