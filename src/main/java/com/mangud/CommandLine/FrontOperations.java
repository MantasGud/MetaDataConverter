package com.mangud.CommandLine;

import com.mangud.Metadata.MetaDataProcessor;
import com.mangud.States.MetadataToolState;

import java.net.UnknownHostException;
import java.util.Scanner;

import static com.mangud.States.MetadataToolState.saveState;

public class FrontOperations {
    public static void runTool(MetadataToolState state) throws UnknownHostException {
        Scanner scanner = new Scanner(System.in);
        MetaDataProcessor processor = new MetaDataProcessor();
        state.setStart(true);
        System.out.println("Welcome to the database metadata tool.");

        while (state.isStart()) {
            displayMainMenu();
            BackOperations.processScannerFirstLayer(scanner, state, processor);
        }

        if (!state.isStart()) {
            System.out.println("Exiting...");
            saveState(state);
            System.exit(0);
        }
    }
    private static void displayMainMenu() {
        System.out.println("\nPlease choose an option:");
        System.out.println("1. Start process metadata to DDL/.csv file");
        System.out.println("2. Test connection to server");
        System.out.println("3. Print all states");
        System.out.println("4. Set settings");
        System.out.println("5. Insert created DDL file to database(ORACLE)");
        System.out.println("6. Exit");
        System.out.println("7. Compare schema(Oracle) to schema(Oracle) and show diffs in file.");
    }
    static void optionLayer(Scanner scanner, MetadataToolState state) {
        int choice = 0;
        while (choice != 6) {

            displaySettingsMenu();
            choice = Integer.parseInt(scanner.nextLine());
            BackOperations.processScannerOptionLayer(choice, scanner, state);
        }
    }
    private static void displaySettingsMenu() {
        System.out.println("Settings");
        System.out.println("1. Choose database type");
        System.out.println("2. Set database connection details");
        System.out.println("3. Set output file");
        System.out.println("4. Set table options");
        System.out.println("5. DDL options");
        System.out.println("6. Return");
    }
    public static void printDatabaseTypes() {
        System.out.println("Choose a database type:");
        System.out.println("1. Oracle");
        System.out.println("2. MySQL");
        System.out.println("3. SQL Server");
        System.out.println("4. DB2");
        System.out.println("5. AS400");
    }
    public static void printStatesInfo(String states) {
        System.out.println("States :");
        System.out.println(states);
    }
    public static void tableLayer(Scanner scanner, MetadataToolState state) {

        int choice = 0;
        while (choice != 5) {
            displayTableSettingsMenu();
            choice = Integer.parseInt(scanner.nextLine());
            BackOperations.processScannerTableOptionLayer(choice, scanner, state);

        }
    }
    private static void displayTableSettingsMenu() {
        System.out.println("Table settings");
        System.out.println("1. To read whole schema");
        System.out.println("2. To read table list");
        System.out.println("3. Print all tables in table file");
        System.out.println("4. Set table file name");
        System.out.println("5. Return");
    }
    public static void printAllTablesFromFile(String tableNames) {
        System.out.println("All tables in file : ");
        System.out.println(tableNames);
    }

    public static void ddlLayer(Scanner scanner, MetadataToolState state) {
        int choice = 0;
        while (choice != 6) {
            displayDDLSettingsMenu();
            choice = Integer.parseInt(scanner.nextLine());
            BackOperations.processScannerDDLOptionLayer(choice, scanner, state);

        }
    }

    private static void displayDDLSettingsMenu() {
        System.out.println("DDL settings");
        System.out.println("1. Set new DDL database type");
        System.out.println("2. Set DDL schema name");
        System.out.println("3. Set added column name");
        System.out.println("4. Set table prefix");
        System.out.println("5. Set tablespace name");
        System.out.println("6. Return");
    }
}
