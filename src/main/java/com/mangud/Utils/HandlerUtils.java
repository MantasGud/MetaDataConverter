package com.mangud.Utils;

import com.mangud.DDLCreators.AS400DDL;
import com.mangud.DDLCreators.DB2DDL;
import com.mangud.DDLCreators.DDLHandler;
import com.mangud.DDLCreators.OracleDDL;
import com.mangud.Enums.DatabaseType;
import com.mangud.Handlers.*;
import lombok.experimental.UtilityClass;

import static com.mangud.constants.ErrorsConstants.*;

@UtilityClass
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
            case MYSQL -> throw new UnsupportedOperationException(NOT_IMPLEMENTED);
            case SQL -> throw new UnsupportedOperationException(NOT_IMPLEMENTED);
            case DB2 -> new DB2DDL();
            case AS400 -> new AS400DDL();
        };

    }

}
