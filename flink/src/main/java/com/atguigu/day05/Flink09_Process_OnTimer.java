package com.atguigu.day05;

import com.atguigu.bean.WaterSensor1;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class Flink09_Process_OnTimer {
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

        // 3. 使用ProcessFunction的定时器功能
        // 定时器只能用在KeyedStream流中
        waterSensor1DS.keyBy(WaterSensor1::getId).process(new ProcessFunction<WaterSensor1, WaterSensor1>() {
            @Override
            public void processElement(WaterSensor1 value, Context ctx, Collector<WaterSensor1> out) throws Exception {

                // 获取当前数据的处理时间
                long ts = ctx.timerService().currentProcessingTime();
                System.out.println(ts);

                // 注册定时器
                ctx.timerService().registerProcessingTimeTimer(ts + 1000L);

                // 输出数据
                out.collect(value);

            }

            // 注册的定时器响起，触发动作
            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<WaterSensor1> out) throws Exception {

                System.out.println("定时器触发: " + timestamp);
                long ts = ctx.timerService().currentProcessingTime();
                ctx.timerService().registerProcessingTimeTimer(ts + 1000L);

            }
        }).print();

        // 4. 执行任务
        env.execute();

    }
}
