package com.mangud.Utils;

import java.net.URL;
import java.net.URLClassLoader;

public class DriversUtils {

    public static void configureDriversClasspaths() throws ClassNotFoundException {
        configureClasspath("mysql-connector-j-8.3.0.jar", "com.mysql.cj.jdbc.Driver");
        configureClasspath("jt400-20.0.6.jar", "com.ibm.as400.access.AS400JDBCDriver");
        configureClasspath("db2jcc4-4.33.31.jar", "com.ibm.db2.jcc.DB2Driver");
        configureClasspath("ojdbc10-19.21.0.0.jar", "oracle.jdbc.driver.OracleDriver");
        configureClasspath("mssql-jdbc-12.4.2.jre11.jar", "com.microsoft.sqlserver.jdbc.SQLServerDriver");
    }

    private static void configureClasspath(String resourceLocation, String className)
            throws ClassNotFoundException {
        URL resourceUrl = com.mangud.Main.class.getClassLoader().getResource(resourceLocation);

        if (resourceUrl == null) {
            System.err.println("Resource not found: " + resourceLocation);
            return;
        }

        URL[] urls = {resourceUrl};
        URLClassLoader loader = new URLClassLoader(urls, Thread.currentThread().getContextClassLoader());
        Thread.currentThread().setContextClassLoader(loader);
        Class.forName(className, true, loader);
    }
}
