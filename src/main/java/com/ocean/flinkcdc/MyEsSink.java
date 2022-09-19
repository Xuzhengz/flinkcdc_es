package com.ocean.flinkcdc;

import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
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
 * <p>
 * 写或更新es自定义的sink
 */
public class MyEsSink extends RichSinkFunction<JSONObject> {
    static RestHighLevelClient esClient;

    @Override
    public void open(Configuration parameters) throws Exception {
        //创建客户端对象
        esClient = new RestHighLevelClient(RestClient.builder(new HttpHost("172.16.8.181", 9200)));
    }

    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
//        批量新增文档
        BulkRequest request = new BulkRequest();
        request.add(new IndexRequest().index("sh_address_db_cdc").type("sh_dzs").id(String.valueOf(value.get("location_id"))).source(value));
        BulkResponse response = esClient.bulk(request, RequestOptions.DEFAULT);
        if (response.hasFailures() == false) {
            System.out.println("操作成功" + "\t花费时长：" + response.getTook() + "\t主键id：" + value.get("location_id"));
        } else {
            System.out.println(response.buildFailureMessage());
        }
    }

    @Override
    public void close() throws Exception {
        if (esClient != null) {
            try {
                esClient.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 自定义删除es的sink
     */
    public static class MyEsDeleteSink extends RichSinkFunction<JSONObject> {
        static RestHighLevelClient esClient;

        @Override
        public void open(Configuration parameters) throws Exception {
            //创建客户端对象
            esClient = new RestHighLevelClient(RestClient.builder(new HttpHost("172.16.8.181", 9200)));
        }

        @Override
        public void invoke(JSONObject value, Context context) throws Exception {
            DeleteRequest deleteRequest = new DeleteRequest("sh_address_db_cdc", "sh_dzs", String.valueOf(value.get("location_id")));
            DeleteResponse deleteResult = esClient.delete(deleteRequest, RequestOptions.DEFAULT);
            System.out.println("操作类型：" + deleteResult.getResult() + "  删除数据id为：" + value.get("location_id"));
        }

        @Override
        public void close() throws Exception {
            if (esClient != null) {
                try {
                    esClient.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

        }
    }
}


