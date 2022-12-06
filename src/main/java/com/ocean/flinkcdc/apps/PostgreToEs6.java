package com.ocean.flinkcdc.apps;

import com.alibaba.fastjson.JSONObject;
import com.ocean.flinkcdc.bean.Bzdz;
import com.ocean.flinkcdc.common.Constants;
import com.ocean.flinkcdc.function.ElasticsearchSink6;
import com.ocean.flinkcdc.function.MyJsonDebeziumDeserializationSchema;
import com.ocean.flinkcdc.utils.CheckPointFileUtils;
import com.ocean.flinkcdc.utils.PKCS5PaddingUtils;
import com.ververica.cdc.connectors.postgres.PostgreSQLSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.File;
import java.util.Properties;

/**
 * @author 徐正洲
 * @date 2022/9/25-14:11
 * <p>
 * Flink写入es
 */
public class PostgreToEs6 {
    public static void main(String[] args) throws Exception {
        /**
         * 设置配置信息
         */
        Properties properties = new Properties();
        properties.setProperty("debuzium.snapshot.mode", "never");
        /**
         * 设置故障恢复策略
         */
//        Configuration configuration = new Configuration();
//        File checkpoint_path = new File(Constants.CK_PATH);
//        String maxTimeFileName = CheckPointFileUtils.getMaxTimeFileName(checkpoint_path);
//
//        if (maxTimeFileName != null && !"".equalsIgnoreCase(maxTimeFileName.trim())) {
//            configuration.setString("execution.savepoint.path", maxTimeFileName);
//        }
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /**
         *  设置状态后端
         */
//        env.setStateBackend(new EmbeddedRocksDBStateBackend());
//        // 启用 checkpoint,设置触发间隔（两次执行开始时间间隔）
//        env.enableCheckpointing(10000);
////        模式支持EXACTLY_ONCE()/AT_LEAST_ONCE()
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
////        存储位置，FileSystemCheckpointStorage(文件存储)
//        env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage(checkpoint_path.toString()));
////        超时时间，checkpoint没在时间内完成则丢弃
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
////        同时并发数量
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
////        最小间隔时间（前一次结束时间，与下一次开始时间间隔）
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
//        //表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
//        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //RETAIN_ON_CANCELLATION:表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
        //DELETE_ON_CANCELLATION: 表示一旦Flink处理程序被cancel后，会删除Checkpoint数据，只有job执行失败的时候才会保存checkpoint

        /**
         * 1、Flink_CDC_POSTGRESQL 实时加密数据源,source只允许并行度为“1”。
         */
        SourceFunction<JSONObject> sourceFunction = PostgreSQLSource.<JSONObject>builder()
                .hostname(Constants.PG_HOSTNAME)
                .port(Constants.PG_PORT)
                .database(Constants.PG_DATABASE)
                .schemaList(Constants.PG_SCHEMALIST)
                .tableList(Constants.PG_TABLELIST)
                .username(Constants.PG_USERNAME)
                .password(Constants.PG_PASSWORD)
                .slotName(Constants.PG_SLOTNAME)
                .decodingPluginName(Constants.PG_DECODINGPLUGINNAME)
                .deserializer(new MyJsonDebeziumDeserializationSchema())
                .debeziumProperties(properties)
                .build();
        DataStreamSource<JSONObject> pgStream = env.addSource(sourceFunction).setParallelism(1);
        pgStream.print("原始数据>>>>");

        /**
         * 2、无状态计算，解密操作
         */
        SingleOutputStreamOperator<JSONObject> paddingStream = pgStream.rebalance().map(new MapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                JSONObject sourceData = JSONObject.parseObject(jsonObject.getString("data"));

                //重新给JSON赋值解密数据和过滤字段
                JSONObject geoJson = new JSONObject();
                geoJson.put("lat", PKCS5PaddingUtils.decrypt(sourceData.getString("gd_wd"), PKCS5PaddingUtils.EPIDEMIC_KEY));
                geoJson.put("lon", PKCS5PaddingUtils.decrypt(sourceData.getString("gd_jd"), PKCS5PaddingUtils.EPIDEMIC_KEY));


                JSONObject dataJson = new JSONObject();
                dataJson.put("title", PKCS5PaddingUtils.decrypt(sourceData.getString("title"), PKCS5PaddingUtils.EPIDEMIC_KEY));
                dataJson.put("address", PKCS5PaddingUtils.decrypt(sourceData.getString("address"), PKCS5PaddingUtils.EPIDEMIC_KEY));
                dataJson.put("xzqh", PKCS5PaddingUtils.decrypt(sourceData.getString("xzqh"), PKCS5PaddingUtils.EPIDEMIC_KEY));
                dataJson.put("pcs", PKCS5PaddingUtils.decrypt(sourceData.getString("pcs"), PKCS5PaddingUtils.EPIDEMIC_KEY));
                dataJson.put("coordinate02", geoJson);
                dataJson.put("source", PKCS5PaddingUtils.decrypt(sourceData.getString("source"), PKCS5PaddingUtils.EPIDEMIC_KEY));
                dataJson.put("location_id", PKCS5PaddingUtils.decrypt(sourceData.getString("location_id"), PKCS5PaddingUtils.EPIDEMIC_KEY));
                dataJson.put("jdname", PKCS5PaddingUtils.decrypt(sourceData.getString("jdname"), PKCS5PaddingUtils.EPIDEMIC_KEY));
                dataJson.put("jwname", PKCS5PaddingUtils.decrypt(sourceData.getString("jwname"), PKCS5PaddingUtils.EPIDEMIC_KEY));
                dataJson.put("address_type", PKCS5PaddingUtils.decrypt(sourceData.getString("address_type"), PKCS5PaddingUtils.EPIDEMIC_KEY));
                dataJson.put("address_type2", PKCS5PaddingUtils.decrypt(sourceData.getString("address_type2"), PKCS5PaddingUtils.EPIDEMIC_KEY));
                dataJson.put("wg", PKCS5PaddingUtils.decrypt(sourceData.getString("wg"), PKCS5PaddingUtils.EPIDEMIC_KEY));
                dataJson.put("gd_id", PKCS5PaddingUtils.decrypt(sourceData.getString("gd_id"), PKCS5PaddingUtils.EPIDEMIC_KEY));
                dataJson.put("gd_parent", PKCS5PaddingUtils.decrypt(sourceData.getString("gd_parent"), PKCS5PaddingUtils.EPIDEMIC_KEY));
                dataJson.put("active", sourceData.getString("active"));

                jsonObject.put("bzdz", dataJson);
                jsonObject.remove("data");
                return jsonObject;
            }
        }).setParallelism(8);

        paddingStream.print("解密数据>>>>>");


        /**
         * 3、自定义ElasticSearch6 Sink写入。
         */

        paddingStream.rebalance().addSink(new ElasticsearchSink6()).setParallelism(8).name("Es6_Sink");


        /**
         4、启动任务
         */
        env.execute("Ocean_Postgre_To_ES6_Streaming");


    }

}