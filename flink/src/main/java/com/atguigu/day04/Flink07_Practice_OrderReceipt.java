package com.atguigu.day04;

import com.atguigu.bean.OrderEvent1;
import com.atguigu.bean.TxEvent1;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;

public class Flink07_Practice_OrderReceipt {
    public static void main(String[] args) throws Exception {

        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 读取2个文本数据创建流
        DataStreamSource<String> orderStreamDS = env.readTextFile("input_flink/OrderLog.csv");
        DataStreamSource<String> receiptStreamDS = env.readTextFile("input_flink/ReceiptLog.csv");

        // 3. 转换为JavaBean
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
        });

        SingleOutputStreamOperator<TxEvent1> txDS = receiptStreamDS.map(new MapFunction<String, TxEvent1>() {
            @Override
            public TxEvent1 map(String value) throws Exception {
                String[] split = value.split(",");
                return new TxEvent1(split[0], split[1], Long.parseLong(split[2]));
            }
        });

        // 4. 按照TXID进行分组
        // 使用了方法引用语法，目的是简化
        KeyedStream<OrderEvent1, String> orderEvent1StringKeyedStream = orderEvent1DS.keyBy(OrderEvent1::getTxId);
        KeyedStream<TxEvent1, String> txEvent1KeyedStream = txDS.keyBy(TxEvent1::getTxId);

        // 5. 连接两个流
        ConnectedStreams<OrderEvent1, TxEvent1> connectedStreams = orderEvent1StringKeyedStream.connect(txEvent1KeyedStream);

        // 6. 处理两条流的数据
        SingleOutputStreamOperator<Tuple2<OrderEvent1, TxEvent1>> result = connectedStreams.process(new
                MyCoKeyedProcessFunc());

        // 7. 打印结果
        result.print();

        // 8. 执行任务
        env.execute();


    }

    public static class MyCoKeyedProcessFunc extends KeyedCoProcessFunction<String, OrderEvent1, TxEvent1,
            Tuple2<OrderEvent1, TxEvent1>>{

        private HashMap<String, OrderEvent1> orderEvent1HashMap = new HashMap<>();
        private HashMap<String, TxEvent1> txEvent1HashMap = new HashMap<>();

        @Override
        public void processElement1(OrderEvent1 value, Context ctx, Collector<Tuple2<OrderEvent1, TxEvent1>> out)
                throws Exception {

            if (txEvent1HashMap.containsKey(value.getTxId())) {
                TxEvent1 txEvent1 = txEvent1HashMap.get(value.getTxId());
                out.collect(new Tuple2<>(value, txEvent1));
            } else {
                orderEvent1HashMap.put(value.getTxId(), value);
            }

        }

        @Override
        public void processElement2(TxEvent1 value, Context ctx, Collector<Tuple2<OrderEvent1, TxEvent1>> out) throws Exception {

            if (orderEvent1HashMap.containsKey(value.getTxId())) {
                OrderEvent1 orderEvent1 = orderEvent1HashMap.get(value.getTxId());
                out.collect(new Tuple2<>(orderEvent1, value));
            } else {
                txEvent1HashMap.put(value.getTxId(), value);
            }

        }
    }
}
