package com.ocean.flinkcdc;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.postgres.PostgreSQLSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import java.util.*;

/**
 * @author 徐正洲
 * @date 2022/9/12-16:58
 * <p>
 * <p>
 * flink cdc 实现读取postgre变化数据写入es
 * <p>
 * 1、自定义cdc数据源
 * 2、分流--create，update，delete
 * 3、实时增、删、改es
 */
public class PostgreToElasticSearch {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
////        开启 Checkpoint, 每隔 5 秒钟做一次 CK
//        env.enableCheckpointing(60000L);
////        指定 CK 的一致性语义
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
////        设置任务关闭的时候保留最后一次 CK 数据
//        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
////        指定从 CK 自动重启策略
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000L));
////        设置状态后端
//        env.setStateBackend(new FsStateBackend("hdfs://172.16.11.151:8020/flinkCDC"));
////        设置访问 HDFS 的用户名
//        System.setProperty("HADOOP_USER_NAME", "root");
//        自定义cdc读取postgre策略
        Properties properties = new Properties();
        properties.setProperty("debezium.slot.drop.on.stop", "true");
        properties.setProperty("include.schema.changes", "true");

        /**
         1、 自定义cdc数据源
         */
        SourceFunction<JSONObject> sourceFunction = PostgreSQLSource.<JSONObject>builder()
                .hostname("172.16.8.222")
                .port(5432)
                .database("dzsj")
                .schemaList("bzdz")
                .tableList("bzdz.bzdz_all")
                .username("postgres")
                .password("1Qaz2wsx")
                .slotName("flink_cdc_pg_ocean")
                .decodingPluginName("pgoutput")
                .deserializer(new MyJsonDebeziumDeserializationSchema())
                .debeziumProperties(properties)
                .build();

        DataStreamSource<JSONObject> pgStream = env.addSource(sourceFunction);

        /**
         2、分流--测输出流
         */
        OutputTag<JSONObject> createOrUpdate = new OutputTag<JSONObject>("createOrUpdate") {
        };
        OutputTag<JSONObject> delete = new OutputTag<JSONObject>("delete") {
        };

        SingleOutputStreamOperator<JSONObject> mainStream = pgStream.process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject jsonObject, Context context, Collector<JSONObject> collector) throws Exception {
                if ("delete".equals(jsonObject.get("operation"))) {
                    context.output(delete, jsonObject);
                } else if ("create".equals(jsonObject.get("operation")) || "update".equals(jsonObject.getString("operation"))) {
                    context.output(createOrUpdate, jsonObject);
                } else {
                    return;
                }
            }
        });

//        获取测输出流
        DataStream<JSONObject> createOrUpdateStream = mainStream.getSideOutput(createOrUpdate);
        DataStream<JSONObject> deleteStream = mainStream.getSideOutput(delete);

        /**
         3、根据不同流操作写入es
         */
        //增加或修改数据
        SingleOutputStreamOperator<JSONObject> createOrDate = createOrUpdateStream.map(new MapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                JSONObject data = (JSONObject) jsonObject.get("data");
                //重新给JSON赋值解密数据
                data.put("mphid",PKCS5PaddingUtils.decrypt(String.valueOf(data.get("mphid")), PKCS5PaddingUtils.EPIDEMIC_KEY));
                data.put("title",PKCS5PaddingUtils.decrypt(String.valueOf(data.get("title")), PKCS5PaddingUtils.EPIDEMIC_KEY));
                data.put("address",PKCS5PaddingUtils.decrypt(String.valueOf(data.get("address")), PKCS5PaddingUtils.EPIDEMIC_KEY));
                data.put("xzqh",PKCS5PaddingUtils.decrypt(String.valueOf(data.get("xzqh")), PKCS5PaddingUtils.EPIDEMIC_KEY));
                data.put("pcs",PKCS5PaddingUtils.decrypt(String.valueOf(data.get("pcs")), PKCS5PaddingUtils.EPIDEMIC_KEY));
                data.put("gd_jd",PKCS5PaddingUtils.decrypt(String.valueOf(data.get("gd_jd")), PKCS5PaddingUtils.EPIDEMIC_KEY));
                data.put("gd_wd",PKCS5PaddingUtils.decrypt(String.valueOf(data.get("gd_wd")), PKCS5PaddingUtils.EPIDEMIC_KEY));
                data.put("source",PKCS5PaddingUtils.decrypt(String.valueOf(data.get("source")), PKCS5PaddingUtils.EPIDEMIC_KEY));
                data.put("kid",PKCS5PaddingUtils.decrypt(String.valueOf(data.get("kid")), PKCS5PaddingUtils.EPIDEMIC_KEY));
                data.put("location_id",PKCS5PaddingUtils.decrypt(String.valueOf(data.get("location_id")), PKCS5PaddingUtils.EPIDEMIC_KEY));
                return data;
            }
        });


        //删除数据
        SingleOutputStreamOperator<JSONObject> deleteData = deleteStream.map(new MapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                JSONObject data = (JSONObject) jsonObject.get("data");
                //重新给JSON赋值解密数据
                data.put("mphid",PKCS5PaddingUtils.decrypt(String.valueOf(data.get("mphid")), PKCS5PaddingUtils.EPIDEMIC_KEY));
                data.put("title",PKCS5PaddingUtils.decrypt(String.valueOf(data.get("title")), PKCS5PaddingUtils.EPIDEMIC_KEY));
                data.put("address",PKCS5PaddingUtils.decrypt(String.valueOf(data.get("address")), PKCS5PaddingUtils.EPIDEMIC_KEY));
                data.put("xzqh",PKCS5PaddingUtils.decrypt(String.valueOf(data.get("xzqh")), PKCS5PaddingUtils.EPIDEMIC_KEY));
                data.put("pcs",PKCS5PaddingUtils.decrypt(String.valueOf(data.get("pcs")), PKCS5PaddingUtils.EPIDEMIC_KEY));
                data.put("gd_jd",PKCS5PaddingUtils.decrypt(String.valueOf(data.get("gd_jd")), PKCS5PaddingUtils.EPIDEMIC_KEY));
                data.put("gd_wd",PKCS5PaddingUtils.decrypt(String.valueOf(data.get("gd_wd")), PKCS5PaddingUtils.EPIDEMIC_KEY));
                data.put("source",PKCS5PaddingUtils.decrypt(String.valueOf(data.get("source")), PKCS5PaddingUtils.EPIDEMIC_KEY));
                data.put("kid",PKCS5PaddingUtils.decrypt(String.valueOf(data.get("kid")), PKCS5PaddingUtils.EPIDEMIC_KEY));
                data.put("location_id",PKCS5PaddingUtils.decrypt(String.valueOf(data.get("location_id")), PKCS5PaddingUtils.EPIDEMIC_KEY));
                return data;
            }
        });

        createOrDate.print("解密数据：");

        createOrDate.addSink(new MyEsSink());
        deleteData.addSink(new MyEsSink.MyEsDeleteSink());

        env.execute("pg12-es6-job：");

    }

}