package com.mangud.utils;

import com.mangud.Utils.DriversUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.net.URL;
import java.net.URLClassLoader;

import static com.mangud.Utils.DriversUtils.configureClasspath;
import static com.mangud.constants.DriversConstants.*;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
public class DriversUtilsTest {
    private static final String DRIVERS_LOCATION = "";

    private final String[] resourceLocations = {
            DRIVERS_LOCATION + MYSQL_DRIVER_JAR,
            DRIVERS_LOCATION + AS400_DRIVER_JAR,
            DRIVERS_LOCATION + DB2_DRIVER_JAR,
            DRIVERS_LOCATION + ORACLE_DRIVER_JAR,
            DRIVERS_LOCATION + SQL_DRIVER_JAR
    };

    @Test
    public void testResourceAvailability() {
        for (String location : resourceLocations) {
            assertNotNull(getClass().getClassLoader().getResource(location), String.format("Resource %s not found", location));
        }
    }

    @Test
    public void testInvalidClassName() {
        assertThrows(ClassNotFoundException.class,
                ()-> configureClasspath(DRIVERS_LOCATION + MYSQL_DRIVER_JAR, "org.aaa.aaa"));
    }

    @Test
    public void testInvalidResourceFileName() {
        assertNull(getClass().getClassLoader().getResource("aaa.jar"), String.format("Resource %s not found", "aaa.jar"));
    }

    @Test
    public void testClassLoading() throws Exception {

        simulateClassLoading(DRIVERS_LOCATION + MYSQL_DRIVER_JAR, MYSQL_DRIVER_PATH);
        simulateClassLoading(DRIVERS_LOCATION + AS400_DRIVER_JAR, AS400_DRIVER_PATH);
        simulateClassLoading(DRIVERS_LOCATION + DB2_DRIVER_JAR, DB2_DRIVER_PATH);
        simulateClassLoading(DRIVERS_LOCATION + ORACLE_DRIVER_JAR, ORACLE_DRIVER_PATH);
        simulateClassLoading(DRIVERS_LOCATION + SQL_DRIVER_JAR, SQL_DRIVER_PATH);

    }

    private void simulateClassLoading(String resourceLocation, String className) throws Exception {
        URL resourceUrl = getClass().getClassLoader().getResource(resourceLocation);
        assertNotNull(resourceUrl, "Resource URL should not be null");

        URL[] urls = {resourceUrl};
        URLClassLoader loader = new URLClassLoader(urls, getClass().getClassLoader());
        Class.forName(className, true, loader);
    }

    @Test
    public void testConfigureDriversClasspathsLoadedSuccessfully() {
        try {
            DriversUtils.configureDriversClasspaths();
        } catch (ClassNotFoundException e) {
            fail("All classes should be loaded without ClassNotFoundException");
        } catch (ExceptionInInitializerError e) {
            fail("All resources should be found without ExceptionInInitializerError");
        }
    }

}
