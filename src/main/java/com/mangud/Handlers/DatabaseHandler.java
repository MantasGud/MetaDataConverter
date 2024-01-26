package com.mangud.Handlers;

import com.mangud.Metadata.TableMetaData;
import com.mangud.States.MetadataToolState;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

public interface DatabaseHandler {
    TableMetaData extractTableMetadata(DatabaseMetaData metaData, String tableName, MetadataToolState state);
    void processTable(DatabaseMetaData metaData, String tableName, FileWriter writer, FileWriter ddlWriter, MetadataToolState state) throws SQLException, IOException;
}
