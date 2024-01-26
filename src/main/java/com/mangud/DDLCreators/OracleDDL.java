package com.mangud.DDLCreators;

import com.mangud.Metadata.TableMetaData;
import com.mangud.States.MetadataToolState;

import java.io.FileWriter;
import java.util.List;

public class OracleDDL implements DDLHandler{

    @Override
    public void CreateDDLFile(MetadataToolState state, List<TableMetaData> schema, FileWriter ddlWriter) {

    }

}
