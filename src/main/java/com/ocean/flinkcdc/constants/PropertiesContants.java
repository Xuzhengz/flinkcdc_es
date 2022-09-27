package com.ocean.flinkcdc.constants;

import org.apache.http.HttpHost;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Properties;

/**
 * @author 徐正洲
 * @date 2022/9/25-14:20
 *
 *
 * 全局常量类
 */
public class PropertiesContants {
    public static Properties properties = new Properties();
    public static InputStream systemResourceAsStream = PropertiesContants.class.getClassLoader().getSystemResourceAsStream("app.properties");
    static {
        try {
            properties.load(systemResourceAsStream);
            properties.setProperty("debezium.snapshot.mode", "never");
            properties.setProperty("debezium.slot.drop.on.stop", "true");
            properties.setProperty("include.schema.changes", "true");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static final String PGHOSTNAME = properties.getProperty("pg_hostname");
    public static final int PGPORT = Integer.parseInt(properties.getProperty("pg_port"));
    public static final String PGDATABASE = properties.getProperty("pg_database");
    public static final String PGSCHEMALIST = properties.getProperty("pg_schemaList");
    public static final String PGTABLELIST = properties.getProperty("pg_tableList");
    public static final String PGUSERNAME = properties.getProperty("pg_username");
    public static final String PGPASSWORD = properties.getProperty("pg_password");
    public static final String ESHOSTNAME = properties.getProperty("es_hostname");
    public static final int ESPORT = Integer.parseInt(properties.getProperty("es_port"));
    public static final String ESUSERNAME = properties.getProperty("es_username");
    public static final String ESPASSWORD = properties.getProperty("es_password");
    public static final String ESINDEX = properties.getProperty("es_index");
    public static final String ESTYPE = properties.getProperty("es_type");


}