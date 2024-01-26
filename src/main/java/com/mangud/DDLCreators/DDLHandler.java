package com.mangud.DDLCreators;

import com.mangud.Metadata.TableMetaData;
import com.mangud.States.MetadataToolState;

import java.io.FileWriter;
import java.util.List;

public interface DDLHandler {

    void CreateDDLFile(MetadataToolState state, List<TableMetaData> schema, FileWriter ddlWriter);

}
