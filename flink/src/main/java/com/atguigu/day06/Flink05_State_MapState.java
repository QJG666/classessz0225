package com.atguigu.day06;

import com.atguigu.bean.AvgVc1;
import com.atguigu.bean.WaterSensor1;
import com.atguigu.bean.WaterSensor2;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class Flink05_State_MapState {
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

        // 4. 使用状态编程方式实现水位线去重
        keyedStream.process(new KeyedProcessFunction<String, WaterSensor1, WaterSensor1>() {

            // 定义状态
            private MapState<Integer, String> mapState;

            @Override
            public void open(Configuration parameters) throws Exception {

                // 初始化状态
                mapState = getRuntimeContext().getMapState(new MapStateDescriptor<Integer, String>("map-state", Integer.class, String.class));

            }

            @Override
            public void processElement(WaterSensor1 value, Context ctx, Collector<WaterSensor1> out) throws Exception {

                // 取出当前数据中的水位线
                Integer vc = value.getVc();

                // 判断状态中是否包含当前的水位值
                if (!mapState.contains(vc)) {
                    out.collect(value);
                    mapState.put(vc, "aaa");
                }

            }
        }).print();

        // 5. 执行任务
        env.execute();

    }
}
