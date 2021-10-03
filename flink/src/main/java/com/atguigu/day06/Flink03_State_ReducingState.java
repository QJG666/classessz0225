package com.atguigu.day06;

import com.atguigu.bean.WaterSensor1;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class Flink03_State_ReducingState {
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

        // 4. 使用状态编程的方式实现累加传感器的水位线
        keyedStream.process(new KeyedProcessFunction<String, WaterSensor1, WaterSensor1>() {

            // 定义状态
            private ReducingState<WaterSensor1> reducingState;

            @Override
            public void open(Configuration parameters) throws Exception {

                // 初始化状态
                reducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<WaterSensor1>("reduce", new ReduceFunction<WaterSensor1>() {
                    @Override
                    public WaterSensor1 reduce(WaterSensor1 value1, WaterSensor1 value2) throws Exception {
                        return new WaterSensor1(value1.getId(), value2.getTs(), value1.getVc() + value2.getVc());
                    }

                }, WaterSensor1.class));

            }

            @Override
            public void processElement(WaterSensor1 value, Context ctx, Collector<WaterSensor1> out) throws Exception {

                // 将当前数据聚合进状态
                reducingState.add(value);

                // 取出状态中的数据
                WaterSensor1 waterSensor1 = reducingState.get();

                // 输出数据
                out.collect(waterSensor1);

            }
        }).print();

        // 5. 执行任务
        env.execute();

    }
}
