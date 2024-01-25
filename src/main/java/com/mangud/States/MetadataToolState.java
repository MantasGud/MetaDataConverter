package com.mangud.States;

import com.mangud.Enums.DatabaseType;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import static com.mangud.Utils.StringUtils.isNullOrEmpty;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class MetadataToolState implements Serializable {
    private static final long serialVersionUID = 1L;

    private DatabaseType dbType = DatabaseType.ORACLE; //1- Oracle 2-MySQL 3-SQL 4-DB2 5-AS400
    private DatabaseType dbDDLType = DatabaseType.ORACLE; //1- Oracle 2-MySQL 3-SQL 4-DB2 5-AS400
    private String url;
    private String fullUrlWithLibraries;
    private String username;
    private String password;
    private String schema;
    private String outputFile;
    private String tableListFile;
    private List<String> tableNames;
    private int tableReadType = 0; //0 - whole schema; 1-table list

    private boolean start = false;

    private String toSchema;
    private String addedColumnName;
    private String tableStart;
    private String tableSpace;

    @Override
    public String toString() {
        return "MetadataToolState{" +
                "dbType=" + dbType +
                ", dbDDLType='" + dbDDLType + '\'' +
                ", url='" + url + '\'' +
                ", fullUrlWithLibraries='" + fullUrlWithLibraries + '\'' +
                ", username='" + username + '\'' +
                ", password='" + password + '\'' +
                ", schema='" + schema + '\'' +
                ", outputFile='" + outputFile + '\'' +
                ", tableListFile='" + tableListFile + '\'' +
                ", tableNames=" + tableNames +
                ", tableReadType=" + tableReadType +
                ", toSchema='" + toSchema + '\'' +
                ", addedColumnName='" + addedColumnName + '\'' +
                ", tableStart='" + tableStart + '\'' +
                ", tableSpace='" + tableSpace + '\'' +
                '}';
    }

    public void setfullUrlWithLibraries() {
        switch (dbType.getDbType()) {
            case 1:
                this.fullUrlWithLibraries = null;
                break;
            case 2:
                this.fullUrlWithLibraries = null;
                break;
            case 3:
                this.fullUrlWithLibraries = null;
                break;
            case 4:
                this.fullUrlWithLibraries = "jdbc:db2://" + this.url + "/" + this.schema;
                break;
            case 5:
                this.fullUrlWithLibraries = "jdbc:as400://" + this.url + ";libraries=" + this.schema + ";";
                break;
            default:
                this.fullUrlWithLibraries = null;
        }

    }

    public void createNewTableList() {
        this.tableNames = new ArrayList<>();
        System.out.println("Table file name: " + this.tableListFile);
        try (BufferedReader br = new BufferedReader(new FileReader(this.tableListFile))) {
            String line;
            while ((line = br.readLine()) != null) {
                tableNames.add(line.trim());
            }
            System.out.println("Table list updated.");
        } catch (IOException e) {
            System.err.println("Failed to read table list file");
            e.printStackTrace();
        }
    }

    public static MetadataToolState loadState() {
        File stateFile = new File("metadata_tool_state.ser");

        if (stateFile.exists()) {
            try (FileInputStream fis = new FileInputStream(stateFile);
                 ObjectInputStream ois = new ObjectInputStream(fis)) {
                return (MetadataToolState) ois.readObject();
            } catch (IOException | ClassNotFoundException e) {
                System.err.println("Failed to load the saved state. Creating a new state.");
            }
        }

        return new MetadataToolState();
    }

    public static void saveState(MetadataToolState state) {
        try (FileOutputStream fos = new FileOutputStream("metadata_tool_state.ser");
             ObjectOutputStream oos = new ObjectOutputStream(fos)) {
            oos.writeObject(state);
        } catch (IOException e) {
            System.err.println("Failed to save the state.");
        }
    }

    public boolean isValidInfo() {
        this.setfullUrlWithLibraries();
        // Check if the required fields are not null

        if (isNullOrEmpty(schema)) {
            System.err.println("Invalid password.");
            return false;
        }

        if (isNullOrEmpty(password)) {
            System.err.println("Invalid password.");
            return false;
        }

        if (isNullOrEmpty(username)) {
            System.err.println("Invalid username.");
            return false;
        }

        if (isNullOrEmpty(url)) {
            System.err.println("Invalid url.");
            return false;
        }

        if (fullUrlWithLibraries == null) {
            System.err.println("There was an error generating full url. Try again by writing url and port.");
            return false;
        }

        if (isNullOrEmpty(toSchema)) {
            System.err.println("Invalid schema name in generatable DDL.");
            return false;
        }

        // Validate database type
        if (dbType.getDbType() < 1 || dbType.getDbType() > 5) {
            System.err.println("Invalid database type.");
            return false;
        }

        // Validate DLL database type
        if (dbDDLType.getDbType() < 1 || dbDDLType.getDbType() > 5) {
            System.err.println("Invalid DDL database type.");
            return false;
        }

        // Validate tableReadType
        if (tableReadType < 0 || tableReadType > 1) {
            System.err.println("Invalid table read type.");
            return false;
        }

        // Validate outputFile
        if (isNullOrEmpty(outputFile)) {
            System.err.println("Output file name cannot be null or empty.");
            return false;
        }

        // Validate tableListFile and tableNames if tableReadType is 1
        if (tableReadType == 1) {
            if (isNullOrEmpty(tableListFile)) {
                System.err.println("Table list file name cannot be null or empty when tableReadType is 1.");
                return false;
            }
            if (tableNames == null || tableNames.isEmpty()) {
                System.err.println("Table names list cannot be null or empty when tableReadType is 1.");
                return false;
            }
        }

        return true;
    }
}
