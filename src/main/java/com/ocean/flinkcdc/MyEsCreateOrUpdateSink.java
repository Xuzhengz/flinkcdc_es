package com.ocean.flinkcdc;

import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.http.HttpHost;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;


/**
 * @author 徐正洲
 * @create 2022-09-13 11:53
 */
public class MyEsCreateOrUpdateSink extends RichSinkFunction<AddressPoJo> {
    static RestHighLevelClient esClient;

    @Override
    public void open(Configuration parameters) throws Exception {
        //创建客户端对象
        esClient = new RestHighLevelClient(RestClient.builder(new HttpHost("172.16.8.181", 9200)));
    }

    @Override
    public void invoke(AddressPoJo value, Context context) throws Exception {
//         创建文档
        IndexRequest request = new IndexRequest();
        request.index("user").type("address_test").id(value.getLocation_id());
//        插入数据必须转换为json
        ObjectMapper mapper = new ObjectMapper();
        String userJson = mapper.writeValueAsString(value);
        request.source(userJson, XContentType.JSON);
        IndexResponse response = esClient.index(request, RequestOptions.DEFAULT);
        System.out.println("操作类型：" + response.getResult() + "," + "id：" + value.getLocation_id());
    }

    @Override
    public void close() throws Exception {
        esClient.close();
    }

    public static class MyEsDeleteSink extends RichSinkFunction<JSONObject> {
        static RestHighLevelClient esClient;

        @Override
        public void open(Configuration parameters) throws Exception {
            //创建客户端对象
            esClient = new RestHighLevelClient(RestClient.builder(new HttpHost("172.16.8.181", 9200)));
        }

        @Override
        public void invoke(JSONObject value, Context context) throws Exception {
            DeleteRequest deleteRequest = new DeleteRequest("user", "address_test", String.valueOf(value.get("location_id")));
            DeleteResponse deleteResult = esClient.delete(deleteRequest, RequestOptions.DEFAULT);
            System.out.println("操作类型：" + deleteResult.getResult() + "  删除数据id为：" + value.get("location_id"));
        }

        @Override
        public void close() throws Exception {
            esClient.close();
        }
    }
}


