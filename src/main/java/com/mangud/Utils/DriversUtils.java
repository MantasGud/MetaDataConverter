package com.mangud.Utils;

import lombok.experimental.UtilityClass;

import java.net.URL;
import java.net.URLClassLoader;

import static com.mangud.constants.DriversConstants.*;
import static com.mangud.constants.ErrorsConstants.*;

@UtilityClass
public class DriversUtils {

    public static void configureDriversClasspaths() throws ClassNotFoundException {
        configureClasspath(MYSQL_DRIVER_JAR, MYSQL_DRIVER_PATH);
        configureClasspath(AS400_DRIVER_JAR, AS400_DRIVER_PATH);
        configureClasspath(DB2_DRIVER_JAR, DB2_DRIVER_PATH);
        configureClasspath(ORACLE_DRIVER_JAR, ORACLE_DRIVER_PATH);
        configureClasspath(SQL_DRIVER_JAR, SQL_DRIVER_PATH);
    }

    public static void configureClasspath(String resourceLocation, String className)
            throws ClassNotFoundException {
        URL resourceUrl = com.mangud.Main.class.getClassLoader().getResource(resourceLocation);

        try {
            if (resourceUrl == null) {
                throw new ExceptionInInitializerError(String.format(RESOURCE_NOT_FOUND, resourceLocation));
            }
        } catch (ExceptionInInitializerError e) {
            System.err.println(e.getMessage());
            return;
        }


        URL[] urls = {resourceUrl};
        URLClassLoader loader = new URLClassLoader(urls, Thread.currentThread().getContextClassLoader());
        Thread.currentThread().setContextClassLoader(loader);
        Class.forName(className, true, loader);
    }
}
