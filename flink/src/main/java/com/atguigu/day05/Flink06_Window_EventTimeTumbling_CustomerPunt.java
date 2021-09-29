package com.atguigu.day05;

import com.atguigu.bean.WaterSensor1;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class Flink06_Window_EventTimeTumbling_CustomerPunt {
    public static void main(String[] args) throws Exception {

        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 读取端口数据并转换为JavaBean
        SingleOutputStreamOperator<WaterSensor1> waterSensor1DS = env.socketTextStream("hadoop106", 9999)
                .map(data -> {
                    String[] split = data.split(",");
                    return new WaterSensor1(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                });

        // 3. 提取数据中的时间戳字段
        WatermarkStrategy<WaterSensor1> waterSensor1WatermarkStrategy = new WatermarkStrategy<WaterSensor1>() {

            @Override
            public WatermarkGenerator<WaterSensor1> createWatermarkGenerator(WatermarkGeneratorSupplier.Context
                                                                                     context) {
                return new MyPunt(2000L);
            }
        }.withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor1>() {
            @Override
            public long extractTimestamp(WaterSensor1 element, long recordTimestamp) {
                return element.getTs() * 1000L;
            }
        });

        SingleOutputStreamOperator<WaterSensor1> waterSensor1SingleOutputStreamOperator = waterSensor1DS
                .assignTimestampsAndWatermarks(waterSensor1WatermarkStrategy);

        // 4. 按照id分组
        KeyedStream<WaterSensor1, String> keyedStream = waterSensor1SingleOutputStreamOperator
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

    // 自定义周期性的watermark生成器
    public static class MyPunt implements WatermarkGenerator<WaterSensor1>{

        private Long maxTs;
        private Long maxDelay;

        public MyPunt(Long maxDelay) {
            this.maxDelay = maxDelay;
            this.maxTs = Long.MIN_VALUE + maxDelay + 1;
        }

        // 当数据来的时候调用
        @Override
        public void onEvent(WaterSensor1 event, long eventTimestamp, WatermarkOutput output) {
            System.out.println("取数据中最大的时间戳");
            maxTs = Math.max(eventTimestamp, maxTs);
            output.emitWatermark(new Watermark(maxTs - maxDelay));
        }

        // 周期性调用
        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
        }
    }
}
