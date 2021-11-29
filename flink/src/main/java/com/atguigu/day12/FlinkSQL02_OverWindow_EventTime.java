package com.atguigu.day12;

import com.atguigu.bean.WaterSensor1;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

public class FlinkSQL02_OverWindow_EventTime {
    public static void main(String[] args) throws Exception {

        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2. 读取文本数据转换为JavaBean并提取时间戳生成watermark
        WatermarkStrategy<WaterSensor1> waterSensor1WatermarkStrategy = WatermarkStrategy.<WaterSensor1>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor1>() {
                    @Override
                    public long extractTimestamp(WaterSensor1 element, long recordTimestamp) {
                        return element.getTs() * 1000L;
                    }
                });

//        SingleOutputStreamOperator<WaterSensor1> waterSensor1DS = env.readTextFile("input_flink/sensor.txt")
        SingleOutputStreamOperator<WaterSensor1> waterSensor1DS = env.socketTextStream("hadoop106", 9999)
                .map(line -> {
                    String[] split = line.split(",");
                    return new WaterSensor1(split[0],
                            Long.parseLong(split[1]),
                            Integer.parseInt(split[2]));
                }).assignTimestampsAndWatermarks(waterSensor1WatermarkStrategy);

        // 3. 将流转换为表并指定事件时间字段
        Table table = tableEnv.fromDataStream(waterSensor1DS,
                $("id"),
                $("ts"),
                $("vc"),
                $("rt").rowtime());

        // 4. 基于事件时间的OverWindow
        Table result = table.window(Over.partitionBy($("id")).orderBy($("rt")).as("ow"))
                .select($("id"), $("id").count().over($("ow")));

        // 5. 转换为流进行打印
        tableEnv.toAppendStream(result, Row.class).print();

        // 6. 执行任务
        env.execute();

    }
}
