package com.mangud.Utils;

import com.mangud.DDLCreators.AS400DDL;
import com.mangud.DDLCreators.DB2DDL;
import com.mangud.DDLCreators.DDLHandler;
import com.mangud.DDLCreators.OracleDDL;
import com.mangud.Enums.DatabaseType;
import com.mangud.Handlers.*;

public class HandlerUtils {

    public static DatabaseHandler getDatabaseHandler(DatabaseType databaseType) {
        return switch (databaseType) {
            case ORACLE -> new OracleHandler();
            case MYSQL -> new MySqlHandler();
            case SQL -> new SQLHandler();
            case AS400 -> new AS400Handler();
            case DB2 -> new DB2Handler();
        };
    }

    public static DDLHandler getDDLHandler(DatabaseType databaseType) {
        return switch (databaseType) {
            case ORACLE -> new OracleDDL();
            case MYSQL -> throw new UnsupportedOperationException("Not implemented yet.");
            case SQL -> throw new UnsupportedOperationException("Not implemented yet");
            case DB2 -> new DB2DDL();
            case AS400 -> new AS400DDL();
            default -> throw new UnsupportedOperationException("Unsupported database.");
        };

    }

}
