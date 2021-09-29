package com.atguigu.day02;

//import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collections;

public class Flink11_Transform_Split_v10 {
    public static void main(String[] args) throws Exception {

        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 从端口读取数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop106", 9999);

        // 3. 按照水位线的高低分流
//        SplitStream<String> split = socketTextStream.split(new OutputSelector<String>() {
//            @Override
//            public Iterable<String> select(String value) {
//                int vc = Integer.parseInt(value.split(",")[2]);
//                return vc > 30 ? Collections.singletonList("High") : Collections.singletonList("Low");
//            }
//        });
//
//        // 4. 选择流输出
//        DataStream<String> high = split.select("High");
//        DataStream<String> low = split.select("Low");
//        DataStream<String> all = split.select("High", "Low");
//
//        // 5. 打印数据
//        high.print("high");
//        low.print("low");
//        all.print("all");

        // 6. 执行任务
        env.execute();

    }
}
