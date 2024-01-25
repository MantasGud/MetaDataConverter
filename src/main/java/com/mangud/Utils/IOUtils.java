package com.mangud.Utils;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class IOUtils {

    public static void writeToFile(List<String> diffSql, String outputFilePath) throws IOException {
        BufferedWriter bw = new BufferedWriter(new FileWriter(outputFilePath));
        for (String sql : diffSql) {
            bw.write(sql);
            bw.newLine();
        }
        bw.close();
    }

}
