package com.ocean.flinkcdc.View;

import com.alibaba.fastjson.JSONObject;
import com.ocean.flinkcdc.constants.PropertiesContants;
import com.ocean.flinkcdc.sink.ElasticsearchSink6;
import com.ocean.flinkcdc.source.MyJsonDebeziumDeserializationSchema;
import com.ocean.flinkcdc.utils.PKCS5PaddingUtils;
import com.ververica.cdc.connectors.postgres.PostgreSQLSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @author 徐正洲
 * @date 2022/9/25-14:11
 * <p>
 * Flink写入es
 */
public class PostgreToEs6Streaming {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        /**
         * 1、Flink_CDC_POSTGRESQL 实时加密数据源,source只允许并行度为“1”。
         */
        SourceFunction<JSONObject> sourceFunction = PostgreSQLSource.<JSONObject>builder()
                .hostname(PropertiesContants.PGHOSTNAME)
                .port(PropertiesContants.PGPORT)
                .database(PropertiesContants.PGDATABASE)
                .schemaList(PropertiesContants.PGSCHEMALIST)
                .tableList(PropertiesContants.PGTABLELIST)
                .username(PropertiesContants.PGUSERNAME)
                .password(PropertiesContants.PGPASSWORD)
                .slotName("flink_cdc_pg_es")
                .decodingPluginName("pgoutput")
                .deserializer(new MyJsonDebeziumDeserializationSchema())
                .debeziumProperties(PropertiesContants.properties)
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
                jsonObject.put("filterJson",dataJson);
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