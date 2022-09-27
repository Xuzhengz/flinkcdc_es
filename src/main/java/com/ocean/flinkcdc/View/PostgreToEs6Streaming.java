package com.ocean.flinkcdc.View;

import com.alibaba.fastjson.JSONObject;
import com.ocean.flinkcdc.sink.ElasticsearchSink6;
import com.ocean.flinkcdc.source.MyJsonDebeziumDeserializationSchema;
import com.ocean.flinkcdc.utils.PKCS5PaddingUtils;
import com.ververica.cdc.connectors.postgres.PostgreSQLSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.InputStream;
import java.util.Properties;

/**
 * @author 徐正洲
 * @date 2022/9/25-14:11
 * <p>
 * Flink写入es
 */
public class PostgreToEs6Streaming {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        /**
        获取自定义数据源配置
        */
        Properties properties = new Properties();
        InputStream systemResourceAsStream = PostgreToEs6Streaming.class.getClassLoader().getResourceAsStream("app.properties");
        properties.load(systemResourceAsStream);
        properties.setProperty("debezium.snapshot.mode", "never");
        properties.setProperty("debezium.slot.drop.on.stop", "true");
        properties.setProperty("include.schema.changes", "true");
        /**
         * 1、Flink_CDC_POSTGRESQL 实时加密数据源,source只允许并行度为“1”。
         */
        SourceFunction<JSONObject> sourceFunction = PostgreSQLSource.<JSONObject>builder()
                .hostname(properties.getProperty("pg_hostname"))
                .port(Integer.parseInt(properties.getProperty("pg_port")))
                .database(properties.getProperty("pg_database"))
                .schemaList(properties.getProperty("pg_schemaList"))
                .tableList(properties.getProperty("pg_tableList"))
                .username(properties.getProperty("pg_username"))
                .password(properties.getProperty("pg_password"))
                .slotName(properties.getProperty("pg_slotname"))
                .decodingPluginName(properties.getProperty("decodingPluginName"))
                .deserializer(new MyJsonDebeziumDeserializationSchema())
                .debeziumProperties(properties)
                .build();
        DataStreamSource<JSONObject> pgStream = env.addSource(sourceFunction).setParallelism(1);
        /**
         * 2、无状态计算，解密操作
         */

        SingleOutputStreamOperator<JSONObject> PaddingStream = pgStream.map(new MapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                //
                JSONObject data = (JSONObject) jsonObject.get("data");
                //重新给JSON赋值解密数据和过滤字段
                JSONObject dataJson = new JSONObject();
                JSONObject geoJson = new JSONObject();
                geoJson.put("lat", PKCS5PaddingUtils.decrypt(String.valueOf(data.get("gd_wd")), PKCS5PaddingUtils.EPIDEMIC_KEY));
                geoJson.put("lon", PKCS5PaddingUtils.decrypt(String.valueOf(data.get("gd_jd")), PKCS5PaddingUtils.EPIDEMIC_KEY));
                dataJson.put("title", PKCS5PaddingUtils.decrypt(String.valueOf(data.get("title")), PKCS5PaddingUtils.EPIDEMIC_KEY));
                dataJson.put("address", PKCS5PaddingUtils.decrypt(String.valueOf(data.get("address")), PKCS5PaddingUtils.EPIDEMIC_KEY));
                dataJson.put("xzqh", PKCS5PaddingUtils.decrypt(String.valueOf(data.get("xzqh")), PKCS5PaddingUtils.EPIDEMIC_KEY));
                dataJson.put("pcs", PKCS5PaddingUtils.decrypt(String.valueOf(data.get("pcs")), PKCS5PaddingUtils.EPIDEMIC_KEY));
                dataJson.put("coordinate02", geoJson);
                dataJson.put("source", PKCS5PaddingUtils.decrypt(String.valueOf(data.get("source")), PKCS5PaddingUtils.EPIDEMIC_KEY));
                dataJson.put("location_id", PKCS5PaddingUtils.decrypt(String.valueOf(data.get("location_id")), PKCS5PaddingUtils.EPIDEMIC_KEY));
                dataJson.put("jdname", PKCS5PaddingUtils.decrypt(String.valueOf(data.get("jdname")), PKCS5PaddingUtils.EPIDEMIC_KEY));
                dataJson.put("jwname", PKCS5PaddingUtils.decrypt(String.valueOf(data.get("jwname")), PKCS5PaddingUtils.EPIDEMIC_KEY));
                dataJson.put("address_type", PKCS5PaddingUtils.decrypt(String.valueOf(data.get("address_type")), PKCS5PaddingUtils.EPIDEMIC_KEY));
                jsonObject.put("filterJson", dataJson);
                jsonObject.remove("data");
                return jsonObject;
            }
        }).setParallelism(3);


        /**
         * 自定义ElasticSearch6 Sink写入。
         */

        PaddingStream.addSink(new ElasticsearchSink6()).setParallelism(3);


        env.execute("Ocean_Postgre_To_ES6_Streaming");


    }
}