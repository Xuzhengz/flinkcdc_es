package com.ocean.flinkcdc.sink;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import java.io.InputStream;
import java.util.Properties;
/**
 * @author 徐正洲
 * @date 2022/9/25-15:17
 * 升级版自定义Es写入操作。
 */
public class ElasticsearchSink6 extends RichSinkFunction<JSONObject> {
    public static RestHighLevelClient esClient;
    public static final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    static Properties properties;
    /**
     * 定义加密的es客户端
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        ElasticsearchSink6 elasticsearchSink6 = new ElasticsearchSink6();
        properties = new Properties();
        InputStream systemResourceAsStream = elasticsearchSink6.getClass().getClassLoader().getResourceAsStream("app.properties");
        properties.load(systemResourceAsStream);
        System.out.println(systemResourceAsStream);
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(properties.getProperty("es_username"), properties.getProperty("es_password")));  //es账号密码
        esClient = new RestHighLevelClient(RestClient.builder(new HttpHost(properties.getProperty("es_hostname"), Integer.parseInt(properties.getProperty("es_port"))))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        httpClientBuilder.disableAuthCaching();
                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                })
        );
    }

    /**
     * 实时同步postgresql操作
     */
    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        String operation = String.valueOf(value.get("operation"));
        if ("create".equals(operation) || "update".equals(operation)) {
            BulkRequest request = new BulkRequest();
            request.add(new IndexRequest().
                    index(properties.getProperty("es_index"))
                    .type(properties.getProperty("es_type"))
                    .id(value.getJSONObject("filterJson")
                            .getString("location_id"))
                    .source(value.getJSONObject("filterJson"))
            );
            BulkResponse response = esClient.bulk(request, RequestOptions.DEFAULT);
            if (response.hasFailures() == false) {
                System.out.println("操作成功" + "\t花费时长：" + response.getTook() + "\t主键id：" + value.getJSONObject("filterJson").get("location_id"));
            } else {
                System.out.println(response.buildFailureMessage());
            }
        } else if ("delete".equals(operation)) {
            DeleteRequest deleteRequest = new DeleteRequest(properties.getProperty("es_index"),
                    properties.getProperty("es_type"),
                    value.getJSONObject("filterJson").getString("location_id")
            );
            DeleteResponse deleteResult = esClient.delete(deleteRequest, RequestOptions.DEFAULT);
            System.out.println("操作类型：" + deleteResult.getResult() +
                    "  删除数据id为：" + value.getJSONObject("filterJson").getString("location_id"));
        }
    }

    /**
     * 关闭es客户端操作
     */
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