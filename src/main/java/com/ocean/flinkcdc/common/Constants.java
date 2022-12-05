package com.ocean.flinkcdc.common;

import org.apache.flink.api.java.utils.ParameterTool;

import java.io.IOException;
import java.io.InputStream;

/**
 * @author 徐正洲
 * @create 2022-12-04 12:26
 */
public class Constants {
    private static ParameterTool parameterTool = null;

    static {
        try {
            InputStream resourceAsStream = Constants.class.getClassLoader().getResourceAsStream("app.properties");
            parameterTool = ParameterTool.fromPropertiesFile(resourceAsStream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    //TODO Postgresql配置
    public static final String PG_HOSTNAME = parameterTool.get("pg_hostname");
    public static final int PG_PORT = parameterTool.getInt("pg_port");
    public static final String PG_DATABASE = parameterTool.get("pg_database");
    public static final String PG_SCHEMALIST = parameterTool.get("pg_schemaList");
    public static final String PG_TABLELIST = parameterTool.get("pg_tableList");
    public static final String PG_USERNAME = parameterTool.get("pg_username");
    public static final String PG_PASSWORD = parameterTool.get("pg_password");
    public static final String PG_DECODINGPLUGINNAME = parameterTool.get("decodingPluginName");
    public static final String PG_SLOTNAME = parameterTool.get("pg_slotname");

    //TODO ES6.X配置
    public static final String ES_HOSTNAME = parameterTool.get("es_hostname");
    public static final int ES_PORT = parameterTool.getInt("es_port");
    public static final String ES_INDEX = parameterTool.get("es_index");
    public static final String ES_TYPE = parameterTool.get("es_type");
    public static final boolean ES_LDAP = parameterTool.getBoolean("auth");
    public static final String ES_USERNAME = parameterTool.get("es_username");
    public static final String ES_PASSWORD = parameterTool.get("es_password");

    //TODO 脏数据文件配置
    public static final String DIRTY_PATH = parameterTool.get("dirty_path");

    //TODO 检查点文件配置
    public static final String CK_PATH = parameterTool.get("checkpoint_path");
}
