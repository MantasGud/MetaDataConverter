package com.mangud;

import java.net.*;

public class Main
{

    public static void main(String[] args) {
        try {
            com.mangud.States.MetadataToolState state = com.mangud.States.MetadataToolState.loadState();
            com.mangud.Utils.DriversUtils.configureDriversClasspaths();
            com.mangud.CommandLine.FrontOperations.runTool(state);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }



}
