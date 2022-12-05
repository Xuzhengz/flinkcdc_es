package com.ocean.flinkcdc.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * @author 徐正洲
 * @date 2022/9/29-9:19
 */
public class CheckPointFileUtils {
    public static String getMaxTimeFileName(File file) {
        Long fileTime = 0L;
        File fileSubName = null;
        String  fileChildName = null;
        File[] files = file.listFiles();

        for (int i = 0; i < files.length; i++) {
            if (fileTime < files[i].lastModified()) {
                fileTime = files[i].lastModified();
                fileSubName = files[i];
            }
        }
        File[] fileChildNames = fileSubName.listFiles();
        for (int i = 0; i < fileChildNames.length; i++) {
            if (fileChildNames[i].getName().startsWith("chk-")){
                fileChildName =  fileChildNames[i].toString();
            }
        }
        return fileChildName;
    }
}