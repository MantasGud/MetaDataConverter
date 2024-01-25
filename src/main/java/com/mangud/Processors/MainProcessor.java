package com.mangud.Processors;

import com.mangud.States.MetadataToolState;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;

public class MainProcessor {

    public void testConnection(MetadataToolState state) {

       state.setfullUrlWithLibraries();
       if (!state.isValidInfo()) {
           System.err.println("State info is not valid.");
           return;
       }

       tryConnectionToDatabase(state);
    }

    private void tryConnectionToDatabase(MetadataToolState state) {
        try (Connection conn = DriverManager.getConnection(state.getFullUrlWithLibraries(), state.getUsername(), state.getPassword())) {
            DatabaseMetaData metaData = conn.getMetaData();
            if (metaData != null) {
                System.out.println("Connection was successful.");
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

    public void convertMetadata(MetadataToolState state) {
        MetaDataProcessor metadataProcessor = new MetaDataProcessor();
        metadataProcessor.convertMetaData(state);
    }

    public void compareTwoDatabaseSchemas(MetadataToolState state) {
        CompareProcessor compareProcessor = new CompareProcessor();
        compareProcessor.compareTwoSchemas(state);
    }
}
