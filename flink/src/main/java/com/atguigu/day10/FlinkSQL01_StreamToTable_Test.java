package com.atguigu.day10;

import com.atguigu.bean.WaterSensor1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkSQL01_StreamToTable_Test {
    public static void main(String[] args) throws Exception {

        // 1. 获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 读取端口数据创建流并转换为JavaBean
        SingleOutputStreamOperator<WaterSensor1> waterSensor1DS = env.socketTextStream("hadoop106", 9999)
                .map(data -> {
                    String[] split = data.split(",");
                    return new WaterSensor1(split[0],
                            Long.parseLong(split[1]),
                            Integer.parseInt(split[2]));
                });

        // 3. 创建表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 4. 将流转换为动态表
        Table sensorTable = tableEnv.fromDataStream(waterSensor1DS);

        // 5. 使用TableAPI过滤出"ws_001"的数据
//        Table selectTable = sensorTable.where($("id").isEqual("ws_001"))
//                .select($("id"), $("ts"), $("vc"));
        Table selectTable = sensorTable
                .where("id = 'ws_001'")
                .select("id, ts, vc");

        // 6. 将selectTable转换为流进行输出
//        DataStream<Row> rowDataStream = tableEnv.toAppendStream(selectTable, Row.class);
        DataStream<Tuple2<Boolean, Row>> rowDataStream = tableEnv.toRetractStream(selectTable, Row.class);
        rowDataStream.print();

        // 7. 执行任务
        env.execute();

    }
}
