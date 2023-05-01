package com.mangud.Handlers;

import com.mangud.States.MetadataToolState;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

public interface DatabaseHandler {
    void processTable(DatabaseMetaData metaData, String tableName, FileWriter writer, FileWriter ddlWriter, MetadataToolState state) throws SQLException, IOException;
}
