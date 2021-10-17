package com.atguigu.day09;

import com.atguigu.bean.LoginEvent1;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class Flink03_Practice_LoginFailWithCEP {
    public static void main(String[] args) throws Exception {

        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 读取文本数据，转换为JavaBean，提取时间戳生成watermark
        WatermarkStrategy<LoginEvent1> loginEvent1WatermarkStrategy = WatermarkStrategy.<LoginEvent1>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner(new SerializableTimestampAssigner<LoginEvent1>() {
                    @Override
                    public long extractTimestamp(LoginEvent1 element, long recordTimestamp) {
                        return element.getEventTime() * 1000L;
                    }
                });

        SingleOutputStreamOperator<LoginEvent1> loginEvent1DS = env.readTextFile("input_flink/LoginLog.csv")
                .map(data -> {
                    String[] split = data.split(",");
                    return new LoginEvent1(Long.parseLong(split[0]),
                            split[1],
                            split[2],
                            Long.parseLong(split[3]));
                }).assignTimestampsAndWatermarks(loginEvent1WatermarkStrategy);

        // 3. 按照用户ID分组
        KeyedStream<LoginEvent1, Long> keyedStream = loginEvent1DS.keyBy(LoginEvent1::getUserId);

        // 4. 定义模式序列
//        Pattern<LoginEvent1, LoginEvent1> loginEvent1Pattern = Pattern.<LoginEvent1>begin("start").where(new SimpleCondition<LoginEvent1>() {
//            @Override
//            public boolean filter(LoginEvent1 value) throws Exception {
//                return "fail".equals(value.getEventType());
//            }
//        }).next("next").where(new SimpleCondition<LoginEvent1>() {
//            @Override
//            public boolean filter(LoginEvent1 value) throws Exception {
//                return "fail".equals(value.getEventType());
//            }
//        }).within(Time.seconds(5));

        Pattern<LoginEvent1, LoginEvent1> loginEvent1Pattern = Pattern.<LoginEvent1>begin("start").where(new SimpleCondition<LoginEvent1>() {
            @Override
            public boolean filter(LoginEvent1 value) throws Exception {
                return "fail".equals(value.getEventType());
            }
        })
                .times(2) // 默认使用的为宽松近邻
                .consecutive() // 指定使用严格近邻模式
                .within(Time.seconds(5));

        // 5. 将模式序列作用于流上
        PatternStream<LoginEvent1> patternStream = CEP.pattern(keyedStream, loginEvent1Pattern);

        // 6. 提取匹配上的事件
        SingleOutputStreamOperator<String> result = patternStream.select(new LoginFailPatternSelectFunc());

        // 7. 打印结果
        result.print();

        // 8. 执行任务
        env.execute();

    }

    public static class LoginFailPatternSelectFunc implements PatternSelectFunction<LoginEvent1, String> {
        @Override
        public String select(Map<String, List<LoginEvent1>> pattern) throws Exception {

            // 取出数据
            LoginEvent1 start = pattern.get("start").get(0);
            // LoginEvent1 next = pattern.get("next").get(0);
            LoginEvent1 next = pattern.get("start").get(1);

            // 输出结果
            return start.getUserId() + "在 " + start.getEventTime() + " 到 " + next.getEventTime() + " 之间连续登录失败2次!";
        }
    }
}
