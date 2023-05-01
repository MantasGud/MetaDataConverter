package com.mangud.Metadata;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class IndexMetaData {
    private String indexName;
    private boolean unique;
    private List<String> columnList;
}
