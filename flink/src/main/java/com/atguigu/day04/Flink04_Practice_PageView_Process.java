package com.atguigu.day04;

import com.atguigu.bean.UserBehavior1;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class Flink04_Practice_PageView_Process {

    public static void main(String[] args) throws Exception {

        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 读取文本数据
        DataStreamSource<String> readTextFile = env.readTextFile("input_flink/UserBehavior.csv");

        // 3. 转换为JavaBean并过滤出PV的数据
        SingleOutputStreamOperator<UserBehavior1> userBehavior1DS = readTextFile.flatMap(new FlatMapFunction<String,
                UserBehavior1>() {
            @Override
            public void flatMap(String value, Collector<UserBehavior1> out) throws Exception {

                // a. 按照 "," 分割
                String[] split = value.split(",");
                // b. 封装JavaBean对象
                UserBehavior1 userBehavior1 = new UserBehavior1(Long.parseLong(split[0]),
                        Long.parseLong(split[1]),
                        Integer.parseInt(split[2]),
                        split[3],
                        Long.parseLong(split[4]));

                // c. 选择需要输出的数据
                if ("pv".equals(userBehavior1.getBehavior())) {
                    out.collect(userBehavior1);
                }
            }
        });

        // 4. 指定Key分组
        KeyedStream<UserBehavior1, String> keyedStream = userBehavior1DS.keyBy(data -> "PV");

        // 5. 计算总和
        SingleOutputStreamOperator<Integer> result = keyedStream.process(new KeyedProcessFunction<String,
                UserBehavior1, Integer>() {

            Integer count = 0;

            @Override
            public void processElement(UserBehavior1 value, Context ctx, Collector<Integer> out) throws Exception {
                count++;
                out.collect(count);
            }
        });

        // 6. 打印输出
        result.print();

        // 7. 执行任务
        env.execute();

    }

}
