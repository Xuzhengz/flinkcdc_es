package com.ocean.flinkcdc.utils;

import java.io.File;
import java.nio.file.Files;
import java.sql.Timestamp;
import java.text.FieldPosition;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;

/**
 * @author 徐正洲
 * @date 2022/9/29-9:19
 */
public class CheckPointFileUtils {
    public static String getMaxTimeFileName(File file) {
        Long fileTime = 0L;
        File fileSubName = null;
        String fileChildName = null;
        File[] files = file.listFiles();

        if (files != null) {
            for (int i = 0; i < files.length; i++) {
                if (fileTime < files[i].lastModified()) {
                    fileTime = files[i].lastModified();
                    fileSubName = files[i];
                }
            }
            File[] fileChildNames = fileSubName.listFiles();
            for (int i = 0; i < fileChildNames.length; i++) {
                if (fileChildNames[i].getName().startsWith("chk-")) {
                    fileChildName = fileChildNames[i].toString();
                }
            }
            return fileChildName;
        } else {
            return null;
        }
    }
}