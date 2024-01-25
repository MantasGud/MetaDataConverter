package com.mangud.Processors;

import com.mangud.Handlers.AS400ToOracleHandler;
import com.mangud.Handlers.DatabaseHandler;
import com.mangud.Metadata.TableMetaData;
import com.mangud.States.MetadataToolState;

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.*;
import java.util.*;

public class MetaDataProcessor {

    public void convertMetaData(MetadataToolState state){
        state.setfullUrlWithLibraries();
        if (!state.isValidInfo()) {
            System.err.println("State info is not valid.");
            return;
        }

        //List<TableMetaData> schema = extractMetaDataFromDatabase(state);
        //createNewMetadataFiles(state, schema);

        createMetaDataFiles(state);
    }

    private List<TableMetaData> extractMetaDataFromDatabase(MetadataToolState state) {
        return null;
    }

    public DatabaseHandler getDatabaseHandler(int sourceDbType, int targetDbType) {
        if ((sourceDbType == 5 || sourceDbType == 4) & targetDbType == 1) {
            return new AS400ToOracleHandler();
        }

        throw new UnsupportedOperationException("Unsupported database combination.");
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

        DatabaseHandler databaseHandler = getDatabaseHandler(state.getDbType().getDbType(), state.getDbDDLType().getDbType());
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
