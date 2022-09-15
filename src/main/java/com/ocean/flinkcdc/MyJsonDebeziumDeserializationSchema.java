package com.ocean.flinkcdc;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.common.utils.Java;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * @author 徐正洲
 * @date 2022/9/12-17:25
 */
public class MyJsonDebeziumDeserializationSchema implements DebeziumDeserializationSchema<JSONObject> {
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<JSONObject> collector) throws Exception {
        String topic = sourceRecord.topic();
        String[] arr = topic.split("\\.");
        String db = arr[1];
        String tableName = arr[2];
        //获取操作类型 READ DELETE UPDATE CREATE
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        //获取值信息并转换为 Struct 类型
        Struct value = (Struct) sourceRecord.value();
        //获取变化后的数据
        Struct after = value.getStruct("after");
        if (after != null) {
            //创建 JSON 对象用于存储数据信息
            JSONObject afterData = new JSONObject();
            for (Field field : after.schema().fields()) {
                Object o = after.get(field);
                afterData.put(field.name(), o);
            }
            //创建 JSON 对象用于封装最终返回值数据信息
            JSONObject afterResult = new JSONObject();
            afterResult.put("operation", operation.toString().toLowerCase());
            afterResult.put("data", afterData);
            System.out.println(afterResult);
            //发送数据至下游
            collector.collect(afterResult);
        } else {
            Struct before = value.getStruct("before");
            //创建 JSON 对象用于存储数据信息
            JSONObject beforeData = new JSONObject();
            for (Field field : before.schema().fields()) {
                Object o = before.get(field);
                beforeData.put(field.name(), o);
            }
            //创建 JSON 对象用于封装最终返回值数据信息
            JSONObject beforeResult = new JSONObject();
            beforeResult.put("operation", operation.toString().toLowerCase());
            beforeResult.put("data", beforeData);
            System.out.println(beforeResult);
            collector.collect(beforeResult);
        }
    }

    @Override
    public TypeInformation<JSONObject> getProducedType() {
        return TypeInformation.of(JSONObject.class);
    }

}