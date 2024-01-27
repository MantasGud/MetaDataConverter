package com.mangud.Handlers;

import com.mangud.Metadata.TableMetaData;
import com.mangud.States.MetadataToolState;

import java.sql.DatabaseMetaData;

public interface DatabaseHandler {
    TableMetaData extractTableMetadata(DatabaseMetaData metaData, String tableName, MetadataToolState state);
}
