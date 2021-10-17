package com.atguigu.day09;

import com.atguigu.bean.OrderEvent1;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

public class Flink04_Practice_OrderPayWithCEP {

    public static void main(String[] args) throws Exception {

        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 读取文本数据创建流，转换为JavaBean，提取时间戳生成Watermark
        WatermarkStrategy<OrderEvent1> orderEvent1WatermarkStrategy = WatermarkStrategy.<OrderEvent1>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderEvent1>() {
                    @Override
                    public long extractTimestamp(OrderEvent1 element, long recordTimestamp) {
                        return element.getEventTime() * 1000L;
                    }
                });

        SingleOutputStreamOperator<OrderEvent1> orderEvent1DS = env.readTextFile("input_flink/OrderLog.csv")
        // SingleOutputStreamOperator<OrderEvent1> orderEvent1DS = env.socketTextStream("hadoop106", 9999)
                .map(data -> {
                    String[] split = data.split(",");
                    return new OrderEvent1(Long.parseLong(split[0]),
                            split[1],
                            split[2],
                            Long.parseLong(split[3]));
                }).assignTimestampsAndWatermarks(orderEvent1WatermarkStrategy);

        // 3. 按照OrderID进行分组
        KeyedStream<OrderEvent1, Long> keyedStream = orderEvent1DS.keyBy(OrderEvent1::getOrderId);

        // 4. 定义模式序列
        Pattern<OrderEvent1, OrderEvent1> orderEvent1Pattern = Pattern.<OrderEvent1>begin("start").where(new SimpleCondition<OrderEvent1>() {
            @Override
            public boolean filter(OrderEvent1 value) throws Exception {
                return "create".equals(value.getEventType());
            }
        }).followedBy("follow").where(new SimpleCondition<OrderEvent1>() {
            @Override
            public boolean filter(OrderEvent1 value) throws Exception {
                return "pay".equals(value.getEventType());
            }
        }).within(Time.minutes(15));

        // 5. 将模式序列作用于流上
        PatternStream<OrderEvent1> patternStream = CEP.pattern(keyedStream, orderEvent1Pattern);

        // 6. 提取正常匹配上的以及超时事件
        SingleOutputStreamOperator<String> result = patternStream.select(new OutputTag<String>("No Pay") {
                                                                         },
                new OrderPayTimeOutFunc(),
                new OrderPaySelectFunc());

        // 7. 打印数据
        result.print();
        result.getSideOutput(new OutputTag<String>("No Pay") {
        }).print("Time Out");

        // 执行任务
        env.execute();

    }

    public static class OrderPayTimeOutFunc implements PatternTimeoutFunction<OrderEvent1, String> {
        @Override
        public String timeout(Map<String, List<OrderEvent1>> pattern, long timeoutTimestamp) throws Exception {

            // 提取事件
            OrderEvent1 createEvent = pattern.get("start").get(0);

            // 输出结果 侧输出流
            return createEvent.getOrderId() + "在 " + createEvent.getEventTime() + " 创建订单，并在 " + timeoutTimestamp / 1000L + " 超时";
        }
    }

    public static class OrderPaySelectFunc implements PatternSelectFunction<OrderEvent1, String> {
        @Override
        public String select(Map<String, List<OrderEvent1>> pattern) throws Exception {

            // 提取事件
            OrderEvent1 createEvent = pattern.get("start").get(0);
            OrderEvent1 payEvent = pattern.get("follow").get(0);

            // 输出结果 主流
            return createEvent.getOrderId() + "在 " + createEvent.getEventTime() + " 创建订单，并在 " + payEvent.getEventTime() + " 完成支付!!!";
        }
    }

}
