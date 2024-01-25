package com.mangud.Utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class DatabaseUtils {

    public static void executeSqlFile(Connection conn, String sqlFilePath) throws IOException, SQLException {
        try (BufferedReader reader = new BufferedReader(new FileReader(sqlFilePath))) {
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line.trim());
                if (sb.toString().endsWith(";")) {
                    executeStatement(conn, sb.toString());
                    sb.setLength(0);
                }
            }
        }
    }

    private static void executeStatement(Connection conn, String sql) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        } catch (SQLException e) {
            System.err.println("Failed to execute SQL statement:");
            System.err.println(sql);
            throw e;
        }
    }

    public static void addDDLToDatabase(String databaseUrl, String dbUser, String dbPassword, String ddlFileName) {
        try (Connection connection = DriverManager.getConnection(databaseUrl, dbUser, dbPassword);
             Statement statement = connection.createStatement()) {

            StringBuilder sb = new StringBuilder();
            try (BufferedReader br = new BufferedReader(new FileReader(ddlFileName))) {
                String line;
                while ((line = br.readLine()) != null) {
                    sb.append(line);
                }
            } catch (IOException e) {
                e.printStackTrace();
                return;
            }

            // Split the SQL statements and execute them
            String[] sqlStatements = sb.toString().split(";");

            for (String sql : sqlStatements) {
                if (!sql.trim().isEmpty()) {
                    statement.execute(sql.trim());
                }
            }

            System.out.println("Tables and index created successfully!");

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
