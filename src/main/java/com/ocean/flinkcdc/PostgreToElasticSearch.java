package com.ocean.flinkcdc;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.postgres.PostgreSQLSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.checkpoint.Checkpoint;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;

import java.sql.Savepoint;
import java.util.Properties;

/**
 * @author 徐正洲
 * @date 2022/9/12-16:58
 *
 *
 * flink cdc 实现读取postgre变化数据写入es
 *
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

//        properties.setProperty("snapshot.mode", "never");
        properties.setProperty("debezium.slot.drop.on.stop", "true");
        properties.setProperty("include.schema.changes", "true");


//        自定义cdc数据源
        SourceFunction<JSONObject > sourceFunction = PostgreSQLSource.<JSONObject>builder()
                .hostname("172.16.8.222")
                .port(5432)
                .database("dzsj") // monitor postgres database
                .schemaList("bzdz")  // monitor inventory schema
                .tableList("bzdz.bzdz_all") // monitor products table
                .username("postgres")
                .password("1Qaz2wsx")
                .slotName("flink_cdc_postgre")
                .decodingPluginName("pgoutput")
                .deserializer(new MyJsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .debeziumProperties(properties)
                .build();

        DataStreamSource<JSONObject> pgStream = env.addSource(sourceFunction);

//        获取增、删、改类型的数据
        SingleOutputStreamOperator<JSONObject> createStream = pgStream.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                return "create".equals(jsonObject.get("operation"));
            }
        });
        SingleOutputStreamOperator<JSONObject> deleteStream = pgStream.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                return "delete".equals(jsonObject.get("operation"));
            }
        });
        SingleOutputStreamOperator<JSONObject> updateStream = pgStream.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                return "update".equals(jsonObject.get("operation"));
            }
        });


        createStream.print("createStream");
        deleteStream.print("deleteStream");
        updateStream.print("updateStream");

        env.execute();

    }
}