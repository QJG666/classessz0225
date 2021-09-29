package com.atguigu.practice;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.HashSet;

public class Flink02_DistinctBySet {
    public static void main(String[] args) throws Exception {

        HashSet<String> hashSet = new HashSet<>();

        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);

        // 2. 读取端口数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop106", 9999);

        // 3. 压平
        SingleOutputStreamOperator<String> wordDS = socketTextStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] words = value.split(" ");
                for (String word :
                        words) {
                    out.collect(word);
                }
            }
        });

        // 4. 过滤数据
        wordDS.keyBy(x -> x)
                .filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {

                if (hashSet.contains(value)) {
                    return false;
                }else {
                    hashSet.add(value);
                    return true;
                }
            }
        }).print();

        // 5. 执行任务
        env.execute();



    }
}
