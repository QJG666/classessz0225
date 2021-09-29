package com.atguigu.day05;

import com.atguigu.bean.WaterSensor1;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.time.Duration;

public class Flink07_Window_EventTimeTumbling_WaterMarkTrans {
    public static void main(String[] args) throws Exception {

        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // 处理乱序的数据，指定提取WaterMark的方式
        WatermarkStrategy<String> waterSensor1WatermarkStrategy = WatermarkStrategy
                .<String>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                    @Override
                    public long extractTimestamp(String element, long recordTimestamp) {
                        String[] split = element.split(",");
                        return Long.parseLong(split[1]) * 1000L;
                    }
                });

        // 2. 读取端口数据并转换为JavaBean
        SingleOutputStreamOperator<WaterSensor1> waterSensor1DS = env.socketTextStream("hadoop106", 9999)
                .assignTimestampsAndWatermarks(waterSensor1WatermarkStrategy)
                .map(data -> {
                    String[] split = data.split(",");
                    return new WaterSensor1(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                });

        // 4. 按照id分组
        KeyedStream<WaterSensor1, String> keyedStream = waterSensor1DS
                .keyBy(WaterSensor1::getId);

        // 5. 开窗
        WindowedStream<WaterSensor1, String, TimeWindow> window = keyedStream.window(TumblingEventTimeWindows.of(Time
                .seconds(5)));

        // 6. 计算总和
        SingleOutputStreamOperator<WaterSensor1> result = window.sum("vc");

        // 7. 打印
        result.print();

        // 8. 执行任务
        env.execute();


    }
}
