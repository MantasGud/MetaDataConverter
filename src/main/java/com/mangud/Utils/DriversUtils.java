package com.mangud.Utils;

import java.net.URL;
import java.net.URLClassLoader;

public class DriversUtils {

    public static void configureDriversClasspaths() throws ClassNotFoundException {
        configureClasspath("jt400-11.1.jar", "com.ibm.as400.access.AS400JDBCDriver");
        configureClasspath("db2jcc4-4.19.26.jar", "com.ibm.db2.jcc.DB2Driver");
        configureClasspath("ojdbc6-11.2.0.4.jar", "oracle.jdbc.driver.OracleDriver");
        configureClasspath("mssql-jdbc-12.2.0.jre11.jar", "com.microsoft.sqlserver.jdbc.SQLServerDriver");
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
