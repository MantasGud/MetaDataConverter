package com.mangud.Handlers;

import com.mangud.Metadata.TableMetaData;
import com.mangud.States.MetadataToolState;

import java.sql.DatabaseMetaData;

public class OracleHandler implements DatabaseHandler{
    @Override
    public TableMetaData extractTableMetadata(DatabaseMetaData metaData, String tableName, MetadataToolState state) {
        return null;
    }

}
