package com.mangud.Enums;

import lombok.Getter;

@Getter
public enum DatabaseType {
    ORACLE(1, "Oracle"),
    MYSQL(2, "MySQL"),
    SQL(3, "SQL"),
    DB2(4, "DB2"),
    AS400(5, "AS400");

    private final int dbType;
    private final String typeName;

    DatabaseType(int dbType, String typeName) {
        this.dbType = dbType;
        this.typeName = typeName;
    }

    public static DatabaseType setDbTypeFromInt(int dbTypeInt) {
        switch (dbTypeInt) {
            case 1:
                return DatabaseType.ORACLE;
            case 2:
                return DatabaseType.MYSQL;
            case 3:
                return DatabaseType.SQL;
            case 4:
                return DatabaseType.DB2;
            case 5:
                return DatabaseType.AS400;
            default:
                throw new IllegalArgumentException("Invalid database type: " + dbTypeInt);
        }
    }

    @Override
    public String toString() {
        return dbType + "-" + typeName;
    }

}
