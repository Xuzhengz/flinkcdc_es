package com.ocean.flinkcdc.function;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author 徐正洲
 * @create 2022-12-01 11:18
 */
public class PaddAsyncFunction<T> extends RichAsyncFunction<T, T> {

    @Override
    public void open(Configuration parameters) throws Exception {
    }

    @Override
    public void asyncInvoke(T t, ResultFuture<T> resultFuture) throws Exception {

    }
}
