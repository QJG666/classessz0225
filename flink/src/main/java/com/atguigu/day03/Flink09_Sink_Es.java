package com.atguigu.day03;

import com.atguigu.bean.WaterSensor1;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;

public class Flink09_Sink_Es {
    public static void main(String[] args) throws Exception {

        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 读取端口数据并转换为javabean
        SingleOutputStreamOperator<WaterSensor1> waterSensorDS = env.socketTextStream("hadoop106", 9999)
//        SingleOutputStreamOperator<WaterSensor1> waterSensorDS = env.readTextFile("input/sensor.txt")
                .map(new MapFunction<String, WaterSensor1>() {
                    @Override
                    public WaterSensor1 map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new WaterSensor1(split[0],
                                Long.parseLong(split[1]),
                                Integer.parseInt(split[2]));
                    }
                });


        // 3. 将数据写入Es
        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("hadoop106", 9200));

        ElasticsearchSink.Builder<WaterSensor1> waterSensor1Builder =
                new ElasticsearchSink.Builder<WaterSensor1>(httpHosts, new MyEsSinkFunc());

        // 批量提交参数
        waterSensor1Builder.setBulkFlushMaxActions(1);

        ElasticsearchSink<WaterSensor1> elasticsearchSink = waterSensor1Builder.build();

        waterSensorDS.addSink(elasticsearchSink);

        // 4. 执行任务
        env.execute();


    }

    public static class MyEsSinkFunc implements ElasticsearchSinkFunction<WaterSensor1> {
        @Override
        public void process(WaterSensor1 element, RuntimeContext ctx, RequestIndexer indexer) {

            HashMap<String, String> source = new HashMap();
            source.put("ts", element.getTs().toString());
            source.put("vc", element.getVc().toString());

            // 创建Index请求
            IndexRequest indexRequest = Requests.indexRequest()
                    .index("sensor1")
                    .type("_doc")
//                    .id(element.getId())
                    .source(source);

            // 写入Es
            indexer.add(indexRequest);
        }
    }

}
