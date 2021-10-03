package com.atguigu.day06;

import com.atguigu.bean.WaterSensor1;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class Flink02_State_ListState {
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

        // 4. 使用ListState实现每个传感器最高的三个水位线
        keyedStream.map(new RichMapFunction<WaterSensor1, List<WaterSensor1>>() {

            // 定义状态
            private ListState<WaterSensor1> top3State;

            @Override
            public void open(Configuration parameters) throws Exception {
                // 初始化状态
                top3State = getRuntimeContext().getListState(new ListStateDescriptor<WaterSensor1>("list-state", WaterSensor1.class));
            }

            @Override
            public List<WaterSensor1> map(WaterSensor1 value) throws Exception {

                // 将当前数据加入状态
                top3State.add(value);

                // 取出状态中的数据并排序
                ArrayList<WaterSensor1> waterSensor1s = Lists.newArrayList(top3State.get().iterator());
                // 按倒序从大到小排序
                waterSensor1s.sort((o1, o2) -> o2.getVc() - o1.getVc());

                // 判断当前数据是否超过3条，如果超过，则删除最后一条
                if (waterSensor1s.size() > 3) {
                    waterSensor1s.remove(3);
                }

                // 更新状态
                top3State.update(waterSensor1s);

                // 返回数据
                return waterSensor1s;
            }
        }).print();

        // 5. 执行任务
        env.execute();

    }
}
