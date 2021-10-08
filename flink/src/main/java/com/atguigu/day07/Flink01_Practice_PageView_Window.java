package com.atguigu.day07;

import com.atguigu.bean.UserBehavior1;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class Flink01_Practice_PageView_Window {
    public static void main(String[] args) throws Exception {

        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 读取文本数据
        DataStreamSource<String> readTextFile = env.readTextFile("input_flink/UserBehavior.csv");

        // 3. 转换为JavaBean，根据行为过滤数据，并提取时间戳生成WaterMark
        WatermarkStrategy<UserBehavior1> userBehavior1WatermarkStrategy = WatermarkStrategy.<UserBehavior1>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior1>() {
                    @Override
                    public long extractTimestamp(UserBehavior1 element, long recordTimestamp) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        SingleOutputStreamOperator<UserBehavior1> userBehavior1DS = readTextFile.map(data -> {
            String[] split = data.split(",");
            return new UserBehavior1(Long.parseLong(split[0]),
                    Long.parseLong(split[1]),
                    Integer.parseInt(split[2]),
                    split[3],
                    Long.parseLong(split[4]));
        }).filter(data -> "pv".equals(data.getBehavior()))
                .assignTimestampsAndWatermarks(userBehavior1WatermarkStrategy);

        // 4. 将数据转换为元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> pvDS = userBehavior1DS.map(new MapFunction<UserBehavior1, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(UserBehavior1 value) throws Exception {
                return new Tuple2<>("PV", 1);
            }
        });

        // 5. 开窗并计算结果
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = pvDS.keyBy(data -> data.f0)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .sum(1);

        // 6. 打印数据
        result.print();

        // 7. 执行任务
        env.execute();


    }
}
