package com.mangud.Utils;

import lombok.experimental.UtilityClass;

@UtilityClass
public class StringUtils {

    public static boolean isNullOrEmpty(String str) {
        return str == null || str.isEmpty();
    }
}
