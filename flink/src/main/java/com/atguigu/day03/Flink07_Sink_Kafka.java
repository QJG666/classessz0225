package com.atguigu.day03;

import com.alibaba.fastjson.JSON;
import com.atguigu.bean.WaterSensor1;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class Flink07_Sink_Kafka {
    public static void main(String[] args) throws Exception {

        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 读取端口数据并转换为javabean
        SingleOutputStreamOperator<WaterSensor1> waterSensorDS = env.socketTextStream("hadoop106", 9999)
                .map(new MapFunction<String, WaterSensor1>() {
                    @Override
                    public WaterSensor1 map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new WaterSensor1(split[0],
                                Long.parseLong(split[1]),
                                Integer.parseInt(split[2]));
                    }
                });


        // 3. 将数据转换为json字符串写入Kafka
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop106:9092");

        waterSensorDS.map(new MapFunction<WaterSensor1, String>() {
            @Override
            public String map(WaterSensor1 value) throws Exception {
                return JSON.toJSONString(value);
            }
        }).addSink(new FlinkKafkaProducer<String>("test", new SimpleStringSchema(), properties));

        // 4. 执行任务
        env.execute();


    }
}
