package com.mangud.Processors;

import com.mangud.States.MetadataToolState;
import com.mangud.Utils.IOUtils;

import java.io.IOException;
import java.sql.*;
import java.util.*;

public class CompareProcessor {

    public void compareTwoSchemas(MetadataToolState state) {
        //For testing purposes now.
        //TODO : Make fully functional compare with all database types.
        final String dbUser = "";
        final String dbPassword = "";
        final String dbUrl1 = "jdbc:oracle:thin:@//";
        final String dbUrl2 = "jdbc:oracle:thin:@//";
        final String compareFile = "SchemaCompareDLLAndOracle.txt";
        try (Connection conn1 = DriverManager.getConnection(dbUrl2, dbUser, dbPassword);
             Connection conn2 = DriverManager.getConnection(dbUrl1, dbUser, dbPassword)){

            CompareProcessor compareProcessor = new CompareProcessor();

            List<String> diffSql = compareProcessor.compareSchemasTSTtoDev(conn1, conn2, dbUser);

            IOUtils.writeToFile(diffSql, compareFile);

            conn1.close();
            conn2.close();
        } catch (SQLException | IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public List<String> compareSchemasTSTtoDev(Connection conn, Connection conn2, String schemaName) throws SQLException {
        Map<String, Set<String>> devTables = getSchemaTablesAndIndexes(conn, schemaName);
        Map<String, Set<String>> tstTables = getSchemaTablesAndIndexes(conn2, schemaName);
        List<String> differences = new ArrayList<>();

        for (String tableName : tstTables.keySet()) {
            if (devTables.containsKey(tableName)) {
                Set<String> prdColumns = getTableColumns(conn, schemaName, tableName);
                Set<String> tstColumns = getTableColumns(conn2, schemaName, tableName);

                differences.addAll(compareTableColumns(tableName, tstColumns, prdColumns));

                Set<String> prdIndexes = devTables.get(tableName);
                Set<String> tstIndexes = tstTables.get(tableName);

                for (String indexName : tstIndexes) {
                    if (!prdIndexes.contains(indexName)) {
                        differences.add("Index missing in dev: " + indexName + " for table: " + tableName);
                    }
                }
            } else {
                differences.add("Table missing in dev: " + tableName + ", present in tst");
            }
        }

        // Check for tables present in dev schema but not in tst schema
        for (String tableName : devTables.keySet()) {
            if (!tstTables.containsKey(tableName)) {
                differences.add("Table missing in tst: " + tableName + ", present in dev");
            }
        }

        return differences;
    }

    private static List<String> compareTableColumns(String tableName, Set<String> tstColumns, Set<String> prdColumns) {
        List<String> columnDifferences = new ArrayList<>();

        for (String columnName : tstColumns) {
            if (!prdColumns.contains(columnName)) {
                columnDifferences.add("Column missing in dev: " + columnName + " for table: " + tableName);
            }
        }

        return columnDifferences;
    }

    private Set<String> getTableColumns(Connection conn, String schema, String tableName) throws SQLException {
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

    private Map<String, Set<String>> getSchemaTablesAndIndexes(Connection conn, String schema) throws SQLException {
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
