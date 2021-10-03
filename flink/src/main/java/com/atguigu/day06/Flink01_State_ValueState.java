package com.atguigu.day06;

import com.atguigu.bean.WaterSensor1;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Flink01_State_ValueState {
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

        // 4. 使用RichFunction实现水位线跳变报警需求
        keyedStream.flatMap(new RichFlatMapFunction<WaterSensor1, String>() {

            // 定义状态
            private ValueState<Integer> vcState;

            @Override
            public void open(Configuration parameters) throws Exception {
                // 初始化状态
                vcState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("vc-state", Integer.class));
            }

            @Override
            public void flatMap(WaterSensor1 value, Collector<String> out) throws Exception {

                // 获取状态中的数据
                Integer lastVc = vcState.value();

                // 更新状态
                vcState.update(value.getVc());

                // 当上一次水位线不为null并且出现跳变的时候进行报警
                if (lastVc != null && Math.abs(lastVc-value.getVc()) > 10) {
                    out.collect(value.getId() + "出现水位线跳变!");
                }

            }
        }).print();

        // 5. 执行任务
        env.execute();

    }
}
