package com.mangud.Metadata;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Map;


@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class TableMetaData {
    String tableName;
    Map<String, ColumnMetaData> columnList;
    Map<String, IndexMetaData> indexList;
}
