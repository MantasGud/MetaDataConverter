package com.mangud.Processors;

import com.mangud.DDLCreators.DDLHandler;
import com.mangud.Handlers.*;
import com.mangud.Metadata.ColumnMetaData;
import com.mangud.Metadata.IndexMetaData;
import com.mangud.Metadata.TableMetaData;
import com.mangud.States.MetadataToolState;
import com.mangud.Utils.HandlerUtils;

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

public class MetaDataProcessor {

    public void convertMetaData(MetadataToolState state){
        state.setfullUrlWithLibraries();
        if (!state.isValidInfo()) {
            System.err.println("State info is not valid.");
            return;
        }

        List<TableMetaData> schema = extractMetaDataFromDatabase(state);
        if (schema.isEmpty()) {
            System.err.println("Error - No table extracted.");
            return;
        }

        createMetaDatafiles(state, schema);
    }

    private void createMetaDatafiles(MetadataToolState state, List<TableMetaData> schema) {
        DDLHandler ddlHandler = HandlerUtils.getDDLHandler(state.getDbDDLType());
        try (FileWriter writer = new FileWriter(state.getOutputFile() + ".csv");
             FileWriter ddlWriter = new FileWriter(state.getOutputFile() + "_ddl.sql")) {
            CreateOriginalMetaDataFile(state, schema, writer);
            ddlHandler.CreateDDLFile(state, schema, ddlWriter);
        } catch (IOException e) {
            System.err.println("Failed to write metadata to CSV file");
            e.printStackTrace();
        }
    }

    private void CreateOriginalMetaDataFile(MetadataToolState state, List<TableMetaData> schema, FileWriter csvWriter) {
        try {
            csvWriter.write("Table Name, Column Name, Data Type, Length, Scale, Not Null, Auto Increment, Description\n");
            for (TableMetaData table : schema) {
                for (Map.Entry<String, ColumnMetaData> column : table.getColumnList().entrySet()) {
                    csvWriter.write(String.format("%s,%s,%s,%d,%d,%s,%s,%s,%n",
                            table.getTableName(), column.getValue().getColumnName(), column.getValue().getDataType(), column.getValue().getLength(),
                            column.getValue().getScale(),
                            column.getValue().isNotNull() ? "Yes" : "No",
                            column.getValue().isAutoIncrement() ? "Yes" : "No",
                            column.getValue().getDescription()));
                }
                for (Map.Entry<String, IndexMetaData> index : table.getIndexList().entrySet()) {
                    String indexType = index.getValue().isUnique() ? "CREATE UNIQUE INDEX" : "CREATE INDEX";
                    String columns = index.getValue().getColumnList().stream().map(col -> "\"" + col + "\"").collect(Collectors.joining(", "));

                    String indexStr = String.format("%s %s.%s ON %s.%s (%s);%n", indexType, state.getSchema(), index.getKey(), state.getSchema(), table.getTableName(), columns);

                    csvWriter.write(indexStr);
                }
                csvWriter.write("\n");
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    private List<TableMetaData> extractMetaDataFromDatabase(MetadataToolState state) {
        DatabaseHandler databaseHandler = HandlerUtils.getDatabaseHandler(state.getDbType());
        List<TableMetaData> schema = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection(state.getFullUrlWithLibraries(), state.getUsername(), state.getPassword())) {
            DatabaseMetaData metaData = conn.getMetaData();

            switch (state.getTableReadType()) {
                case 0:
                    schema = extractMetadataFromWholeSchema(metaData, databaseHandler, state);
                    break;
                case 1:
                    schema = extractMetadataFromTableList(metaData, databaseHandler, state);
                    break;
                default:
                    System.out.println("Invalid read choice. Please set correct read setting.");
            }

        } catch (SQLException e) {
            System.err.println("Failed to connect to database");
            try {
                InetAddress localAddress = InetAddress.getLocalHost();
                System.err.println("Address - " + localAddress);
            } catch (UnknownHostException f ) {
                System.err.println("Unknown host exception - " + e.getMessage());
            }
            System.err.println("Url - " + state.getUrl());
            e.printStackTrace();
        }


        return schema;
    }

    private List<TableMetaData> extractMetadataFromTableList(DatabaseMetaData metaData, DatabaseHandler databaseHandler, MetadataToolState state) {
        List<TableMetaData> tableList = new ArrayList<>();
        for (String tableName : state.getTableNames()) {
            TableMetaData tableMetadata = databaseHandler.extractTableMetadata(metaData, tableName, state);
            tableList.add(tableMetadata);
        }
        return tableList;
    }

    private List<TableMetaData> extractMetadataFromWholeSchema(DatabaseMetaData metaData, DatabaseHandler databaseHandler, MetadataToolState state) {
        List<TableMetaData> tableList = new ArrayList<>();
        try (ResultSet tableRs = metaData.getTables(null, state.getSchema(), null, new String[]{"TABLE"})) {
            while (tableRs.next()) {
                String tableName = tableRs.getString("TABLE_NAME");
                TableMetaData tableMetadata = databaseHandler.extractTableMetadata(metaData, tableName, state);
                tableList.add(tableMetadata);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return tableList;
    }



    private void createMetaDataFiles(MetadataToolState state){
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
            try {
                InetAddress localAddress = InetAddress.getLocalHost();
                System.err.println("Address - " + localAddress);
            } catch (UnknownHostException f ) {
                System.err.println("Unknown host exception - " + e.getMessage());
            }
            System.err.println("Url - " + state.getUrl());
            e.printStackTrace();
        }
    }

    private void writeMetadataToFiles(DatabaseMetaData metaData, FileWriter writer, FileWriter ddlWriter, MetadataToolState state) throws SQLException, IOException {
        writer.write("Table Name, Column Name, Data Type, Length, Scale, Not Null, Auto Increment\n");

        DatabaseHandler databaseHandler = HandlerUtils.getDatabaseHandler(state.getDbType().getDbType(), state.getDbDDLType().getDbType());
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

}
