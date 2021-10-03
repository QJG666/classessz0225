package com.atguigu.day06;

import com.atguigu.bean.AvgVc;
import com.atguigu.bean.AvgVc1;
import com.atguigu.bean.WaterSensor1;
import com.atguigu.bean.WaterSensor2;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class Flink04_State_AggState {
    public static void main(String[] args) throws Exception {

        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 1.1 老版本
        // env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        // 2. 读取端口数据并转换为JavaBean
        SingleOutputStreamOperator<WaterSensor1> waterSensor1DS = env.socketTextStream("hadoop106", 9999)
                .map(data -> {
                    String[] split = data.split(",");
                    return new WaterSensor1(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                });

        // 3. 按照传感器ID分组
        KeyedStream<WaterSensor1, String> keyedStream = waterSensor1DS.keyBy(WaterSensor1::getId);

        // 4. 使用状态编程方式实现平均水位
        keyedStream.process(new KeyedProcessFunction<String, WaterSensor1, WaterSensor2>() {

            // 定义状态
            private AggregatingState<Integer, Double> aggregatingState;

            @Override
            public void open(Configuration parameters) throws Exception {

                // 初始化状态
                aggregatingState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<Integer, AvgVc1, Double>("agg-state", new AggregateFunction<Integer, AvgVc1, Double>() {
                    @Override
                    public AvgVc1 createAccumulator() {
                        return new AvgVc1(0, 0);
                    }

                    @Override
                    public AvgVc1 add(Integer value, AvgVc1 accumulator) {
                        return new AvgVc1(accumulator.getVcSum() + value, accumulator.getCount() + 1);
                    }

                    @Override
                    public Double getResult(AvgVc1 accumulator) {
                        return accumulator.getVcSum() * 1D / accumulator.getCount();
                    }

                    @Override
                    public AvgVc1 merge(AvgVc1 a, AvgVc1 b) {
                        return new AvgVc1(a.getVcSum() + b.getVcSum(), a.getCount() + b.getCount());
                    }
                }, AvgVc1.class));

            }

            @Override
            public void processElement(WaterSensor1 value, Context ctx, Collector<WaterSensor2> out) throws Exception {

                // 将当前数据累加进状态
                aggregatingState.add(value.getVc());

                // 取出状态中的数据
                Double avgVc = aggregatingState.get();

                // 输出数据
                out.collect(new WaterSensor2(value.getId(), value.getTs(), avgVc));

            }
        }).print();

        // 5. 执行任务
        env.execute();

    }
}
