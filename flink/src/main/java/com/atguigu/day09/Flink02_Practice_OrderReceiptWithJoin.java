package com.atguigu.day09;

import com.atguigu.bean.OrderEvent1;
import com.atguigu.bean.TxEvent1;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class Flink02_Practice_OrderReceiptWithJoin {

    public static void main(String[] args) throws Exception {

        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 读取2个文本数据创建流
        DataStreamSource<String> orderStreamDS = env.readTextFile("input_flink/OrderLog.csv");
        DataStreamSource<String> receiptStreamDS = env.readTextFile("input_flink/ReceiptLog.csv");

        // 3. 转换为JavaBean，并提取数据中的时间戳生成Watermark
        WatermarkStrategy<OrderEvent1> orderEvent1WatermarkStrategy = WatermarkStrategy.<OrderEvent1>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderEvent1>() {
                    @Override
                    public long extractTimestamp(OrderEvent1 element, long recordTimestamp) {
                        return element.getEventTime() * 1000l;
                    }
                });

        WatermarkStrategy<TxEvent1> txEvent1WatermarkStrategy = WatermarkStrategy.<TxEvent1>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<TxEvent1>() {
                    @Override
                    public long extractTimestamp(TxEvent1 element, long recordTimestamp) {
                        return element.getEventTime() * 1000L;
                    }
                });

        SingleOutputStreamOperator<OrderEvent1> orderEvent1DS = orderStreamDS.flatMap(new FlatMapFunction<String,
                        OrderEvent1>() {
            @Override
            public void flatMap(String value, Collector<OrderEvent1> out) throws Exception {
                String[] split = value.split(",");
                OrderEvent1 orderEvent1 = new OrderEvent1(
                        Long.parseLong(split[0]),
                        split[1],
                        split[2],
                        Long.parseLong(split[3]));
                if ("pay".equals(orderEvent1.getEventType())) {
                    out.collect(orderEvent1);
                }
            }
        }).assignTimestampsAndWatermarks(orderEvent1WatermarkStrategy);

        SingleOutputStreamOperator<TxEvent1> txDS = receiptStreamDS.map(new MapFunction<String, TxEvent1>() {
            @Override
            public TxEvent1 map(String value) throws Exception {
                String[] split = value.split(",");
                return new TxEvent1(split[0], split[1], Long.parseLong(split[2]));
            }
        }).assignTimestampsAndWatermarks(txEvent1WatermarkStrategy);

        // 4. 连接支付流和到账流
        SingleOutputStreamOperator<Tuple2<OrderEvent1, TxEvent1>> result = orderEvent1DS.keyBy(OrderEvent1::getTxId)
                .intervalJoin(txDS.keyBy(TxEvent1::getTxId))
                .between(Time.seconds(-5), Time.seconds(10))
                .process(new PayReceiptJoinProcessFunc());

        // 5. 打印数据
        result.print();

        // 6. 执行任务
        env.execute();


    }

    public static class PayReceiptJoinProcessFunc extends ProcessJoinFunction<OrderEvent1, TxEvent1, Tuple2<OrderEvent1, TxEvent1>>{

        @Override
        public void processElement(OrderEvent1 left, TxEvent1 right, Context ctx, Collector<Tuple2<OrderEvent1, TxEvent1>> out) throws Exception {

            out.collect(new Tuple2<>(left, right));

        }
    }


}
