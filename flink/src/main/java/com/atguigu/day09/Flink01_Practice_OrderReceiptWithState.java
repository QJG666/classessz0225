package com.atguigu.day09;

import com.atguigu.bean.OrderEvent1;
import com.atguigu.bean.TxEvent1;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class Flink01_Practice_OrderReceiptWithState {

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
        SingleOutputStreamOperator<Tuple2<OrderEvent1, TxEvent1>> result = orderEvent1DS.connect(txDS)
                .keyBy("txId", "txId")
                .process(new OrderReceiptKeyedProcessFunc());

        // 5. 打印数据
        result.print();
        result.getSideOutput(new OutputTag<String>("Payed No Receipt"){}).print("No Receipt");
        result.getSideOutput(new OutputTag<String>("Receipt No Payed"){}).print("No Payed");

        // 6. 执行任务
        env.execute();


    }

    public static class OrderReceiptKeyedProcessFunc extends KeyedCoProcessFunction<String, OrderEvent1, TxEvent1, Tuple2<OrderEvent1, TxEvent1>>{

        // 声明状态
        private ValueState<OrderEvent1> payEventState;
        private ValueState<TxEvent1> txEvent1State;
        private ValueState<Long> timerState;

        @Override
        public void open(Configuration parameters) throws Exception {

            payEventState = getRuntimeContext().getState(new ValueStateDescriptor<OrderEvent1>("pay-state", OrderEvent1.class));

            txEvent1State = getRuntimeContext().getState(new ValueStateDescriptor<TxEvent1>("tx-state", TxEvent1.class));

            timerState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-state", Long.class));

        }

        @Override
        public void processElement1(OrderEvent1 value, Context ctx, Collector<Tuple2<OrderEvent1, TxEvent1>> out) throws Exception {

            // 取出到账状态数据
            TxEvent1 txEvent1 = txEvent1State.value();

            // 判断到账数据是否已经到达
            if (txEvent1 == null) {// 到账数据还没有到达

                // 将自身存入状态
                payEventState.update(value);

                // 注册定时器
                long ts = (value.getEventTime() + 10) * 1000L;
                ctx.timerService().registerEventTimeTimer(ts);
                timerState.update(ts);

            } else {// 到账数据已经到达

                // 结合写入主流
                out.collect(new Tuple2<>(value, txEvent1));

                // 删除定时器
                ctx.timerService().deleteEventTimeTimer(timerState.value());

                // 清空状态
                txEvent1State.clear();
                timerState.clear();

            }

        }

        @Override
        public void processElement2(TxEvent1 value, Context ctx, Collector<Tuple2<OrderEvent1, TxEvent1>> out) throws Exception {

            // 取出支付数据
            OrderEvent1 orderEvent1 = payEventState.value();

            // 判断支付数据是否已经到达
            if (orderEvent1 == null) {// 支付数据没有到达

                // 将自身保存至状态
                txEvent1State.update(value);

                // 注册定时器
                long ts = (value.getEventTime() + 5) * 1000L;
                ctx.timerService().registerEventTimeTimer(ts);
                timerState.update(ts);

            } else {// 支付数据已经到达

                // 结合写出
                out.collect(new Tuple2<>(orderEvent1, value));

                // 删除定时器
                ctx.timerService().deleteEventTimeTimer(timerState.value());

                // 清空状态
                payEventState.clear();
                timerState.clear();

            }

        }

        // 定时器方法
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<OrderEvent1, TxEvent1>> out) throws Exception {

            // 取出支付状态数据
            OrderEvent1 orderEvent1 = payEventState.value();
            TxEvent1 txEvent1 = txEvent1State.value();

            // 判断orderEvent1是否为Null
            if (orderEvent1 != null) {

                ctx.output(new OutputTag<String>("Payed No Receipt"){},
                        orderEvent1.getTxId() + "只有支付没有到账数据");

            } else {
                ctx.output(new OutputTag<String>("Receipt No Payed"){},
                        txEvent1.getTxId() + "只有到账没有支付数据");
            }

            // 清空状态
            payEventState.clear();
            txEvent1State.clear();
            timerState.clear();

        }
    }

}
