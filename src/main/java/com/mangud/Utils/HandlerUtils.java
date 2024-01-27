package com.mangud.Utils;

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
            /*case AS400:
                return new AS400Handler();
            case DB2:
                return new DB2Handler();*/
            default -> throw new UnsupportedOperationException("Unsupported database.");
        };

    }

}
