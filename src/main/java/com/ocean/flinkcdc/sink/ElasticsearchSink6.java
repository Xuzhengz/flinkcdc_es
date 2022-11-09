package com.ocean.flinkcdc.sink;

import com.alibaba.fastjson.JSONObject;
import com.ocean.flinkcdc.utils.DateFormatUtil;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
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
    public static FileWriter fileWriter = null;
    public static File dirtyFile = null;
    private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchSink6.class);


    /**
     * 定义加密的es客户端
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        ElasticsearchSink6 elasticsearchSink6 = new ElasticsearchSink6();
        properties = new Properties();
        InputStream systemResourceAsStream = elasticsearchSink6.getClass().getClassLoader().getResourceAsStream("app.properties");
        properties.load(systemResourceAsStream);
        if (properties.getProperty("auth").equals("enable")) {
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(properties.getProperty("es_username"), properties.getProperty("es_password")));  //es账号密码
            esClient = new RestHighLevelClient(RestClient.builder(new HttpHost(properties.getProperty("es_hostname"), Integer.parseInt(properties.getProperty("es_port"))))
                    .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                        public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                            httpClientBuilder.disableAuthCaching();
                            return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                        }
                    })
            );
        } else {
            esClient = new RestHighLevelClient(RestClient.builder(new HttpHost(properties.getProperty("es_hostname"), Integer.parseInt(properties.getProperty("es_port")))));
        }

        //创建脏数据输出文件流
        String dirty_path = properties.getProperty("dirty_path");
        dirtyFile = new File(dirty_path);
//        2、创建输入输出流
        fileWriter = new FileWriter(dirtyFile, true);
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
                LOG.info("操作成功" + "\t花费时长：" + response.getTook() + "\t主键id：" + value.getJSONObject("filterJson").get("location_id"));
            } else {
                try {
                    LOG.error("同步失败，主键id：" + value.getJSONObject("filterJson").get("location_id"));
                    fileWriter.write(DateFormatUtil.toDate(System.currentTimeMillis()) + "-->" + value + "\n");
                } catch (IOException e) {
                    e.printStackTrace();
                }
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
     * 关闭连接
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
        if (fileWriter != null) {
            try {
                fileWriter.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}