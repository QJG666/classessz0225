package com.atguigu.day11;

import com.atguigu.bean.WaterSensor1;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkSQL01_ProcessTime_StreamToTable {
    public static void main(String[] args) {

        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2. 读取文本数据创建流并转换为JavaBean对象
        SingleOutputStreamOperator<WaterSensor1> waterSensor1DS = env.readTextFile("input_flink/sensor.txt")
                .map(line -> {
                    String[] split = line.split(",");
                    return new WaterSensor1(split[0],
                            Long.parseLong(split[1]),
                            Integer.parseInt(split[2]));
                });

        // 3. 将流转换为表，并指定处理时间
        Table table = tableEnv.fromDataStream(waterSensor1DS,
                $("id"),
                $("ts"),
                $("vc"),
                $("pt").proctime());

        // 4. 打印元数据信息
        table.printSchema();

    }
}
