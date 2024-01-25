package com.mangud.CommandLine;

import com.mangud.Enums.DatabaseType;
import com.mangud.Processors.MainProcessor;
import com.mangud.States.MetadataToolState;
import com.mangud.Utils.DatabaseUtils;

import java.util.Scanner;

public class BackOperations {
    public static void processScannerOptionLayer(int choice, Scanner scanner, MetadataToolState state) {
        try {
            switch (choice) {
                case 1:
                    FrontOperations.printDatabaseTypes();
                    int dbTypeInput = Integer.parseInt(scanner.nextLine());
                    state.setDbType(DatabaseType.setDbTypeFromInt(dbTypeInput));
                    break;
                case 2:
                    setDatabaseConnectionDetails(scanner, state);
                    break;
                case 3:
                    System.out.println("Enter the output file base name:");
                    state.setOutputFile(scanner.nextLine());
                    break;
                case 4:
                    FrontOperations.tableLayer(scanner, state);
                    break;
                case 5:
                    FrontOperations.ddlLayer(scanner, state);
                    break;
                case 6:
                    break;
                default:
                    System.out.println("Invalid choice. Please try again.");
            }
        } catch (NumberFormatException e) {
            System.out.println("Invalid input. Please try again.");
        }
    }
    private static void setDatabaseConnectionDetails(Scanner scanner, MetadataToolState state) {
        System.out.println("Enter the JDBC URL:PORT");
        state.setUrl(scanner.nextLine());
        System.out.println("Enter the username:");
        state.setUsername(scanner.nextLine());
        System.out.println("Enter the password:");
        state.setPassword(scanner.nextLine());
        System.out.println("Enter the schema:");
        state.setSchema(scanner.nextLine());
    }

    public static void processScannerFirstLayer(Scanner scanner, MetadataToolState state, MainProcessor processor) {

        try {

            switch (Integer.parseInt(scanner.nextLine())) {
                case 1:
                    processor.convertMetadata(state);
                    break;
                case 2:
                    processor.testConnection(state);
                    break;
                case 3:
                    FrontOperations.printStatesInfo(state.toString());
                    break;
                case 4:
                    FrontOperations.optionLayer(scanner, state);
                    break;
                case 5:
                    BackOperations.processScannerInsertDLL(scanner);
                    break;
                case 6:
                    state.setStart(false);
                    break;
                case 7:
                    processor.compareTwoDatabaseSchemas(state);
                    break;
                default:
                    System.out.println("Invalid choice. Please try again.");
            }
        } catch (NumberFormatException e) {
            System.out.println("Invalid input. Please try again.");
        }

    }

    private static void processScannerInsertDLL(Scanner scanner) {
        System.out.println("Enter the ORACLE URL");
        String ip = (scanner.nextLine());
        System.out.println("Enter the ORACLE PORT");
        String port = (scanner.nextLine());
        String databaseUrl = "jdbc:oracle:thin:@//" + ip + ":" + port + "/" + ip ;
        System.out.println("Enter the username:");
        String dbUser = (scanner.nextLine());
        System.out.println("Enter the password:");
        String dbPassword = (scanner.nextLine());
        System.out.println("Enter the file name:");
        String DDLFileName = (scanner.nextLine());
        DatabaseUtils.addDDLToDatabase(databaseUrl, dbUser, dbPassword, DDLFileName);
    }

    public static void processScannerTableOptionLayer(int choice, Scanner scanner, MetadataToolState state) {
        try {

            switch (choice) {
                case 1:
                    state.setTableReadType(0);
                    System.out.println("Read set to whole schema");
                    break;
                case 2:
                    state.setTableReadType(1);
                    System.out.println("Read set to table list");
                    break;
                case 3:
                    FrontOperations.printAllTablesFromFile(state.getTableNames().toString());
                    break;
                case 4:
                    System.out.println("Write full file name");
                    state.setTableListFile(scanner.nextLine());
                    state.createNewTableList();
                    break;
                case 5:

                    break;
                default:
                    System.out.println("Invalid choice. Please try again.");
            }
        } catch (NumberFormatException e) {
            System.out.println("Invalid input. Please try again.");
        }
    }

    public static void processScannerDDLOptionLayer(int choice, Scanner scanner, MetadataToolState state) {

        try {

            switch (choice) {
                case 1:
                    FrontOperations.printDatabaseTypes();
                    int dbTypeInput = Integer.parseInt(scanner.nextLine());
                    state.setDbDDLType(DatabaseType.setDbTypeFromInt(dbTypeInput));
                    break;
                case 2:
                    System.out.println("Write schema name in created DDL");
                    state.setToSchema(scanner.nextLine());
                    break;
                case 3:
                    System.out.println("Write added column name in created DDL");
                    state.setAddedColumnName(scanner.nextLine());
                    break;
                case 4:
                    System.out.println("Write prefix of table in created DDL");
                    state.setTableStart(scanner.nextLine());
                    break;
                case 5:
                    System.out.println("Write tablespace in created DDL");
                    state.setTableSpace(scanner.nextLine());
                    break;
                case 6:

                    break;
                default:
                    System.out.println("Invalid choice. Please try again.");
            }
        } catch (NumberFormatException e) {
            System.out.println("Invalid input. Please try again.");
        }
    }
}
