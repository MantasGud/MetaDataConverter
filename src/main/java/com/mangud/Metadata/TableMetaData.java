package com.mangud.Metadata;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class TableMetaData {
    private String columnName;
    private String dataType;
    private int length;
    private int scale;
    private boolean notNull;
    private boolean autoIncrement;
    private String description;

}
