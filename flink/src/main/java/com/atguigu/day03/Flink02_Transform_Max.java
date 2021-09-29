package com.atguigu.day03;

import com.atguigu.bean.WaterSensor1;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink02_Transform_Max {
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

        // 3. 按照传感器ID分组
        KeyedStream<WaterSensor1, String> keyedStream = waterSensorDS.keyBy(new KeySelector<WaterSensor1, String>() {
            @Override
            public String getKey(WaterSensor1 value) throws Exception {
                return value.getId();
            }
        });

        // 4. 计算最高水位线
        SingleOutputStreamOperator<WaterSensor1> result = keyedStream.max("vc");

        // 5. 打印
        result.print();

        // 6. 执行任务
        env.execute();


    }
}
