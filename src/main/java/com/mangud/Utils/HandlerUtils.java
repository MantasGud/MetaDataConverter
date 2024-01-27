package com.mangud.Utils;

import com.mangud.DDLCreators.DDLHandler;
import com.mangud.DDLCreators.OracleDDL;
import com.mangud.Enums.DatabaseType;
import com.mangud.Handlers.AS400Handler;
import com.mangud.Handlers.DB2Handler;
import com.mangud.Handlers.DatabaseHandler;

public class HandlerUtils {

    public static DatabaseHandler getDatabaseHandler(DatabaseType databaseType) {
        return switch (databaseType) {
            /*case ORACLE:
                return new OracleHandler();*/
            case AS400 -> new AS400Handler();
            case DB2 -> new DB2Handler();
            default -> throw new UnsupportedOperationException("Unsupported database.");
        };
    }

    public static DDLHandler getDDLHandler(DatabaseType databaseType) {
        return switch (databaseType) {
            case ORACLE -> new OracleDDL();
            /*case AS400:
                return new AS400Handler();
            case DB2:
                return new DB2Handler();*/
            default -> throw new UnsupportedOperationException("Unsupported database.");
        };

    }

}
