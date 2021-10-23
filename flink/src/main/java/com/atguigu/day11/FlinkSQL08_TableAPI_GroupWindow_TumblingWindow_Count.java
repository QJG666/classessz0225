package com.atguigu.day11;

import com.atguigu.bean.WaterSensor1;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Session;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.*;

public class FlinkSQL08_TableAPI_GroupWindow_TumblingWindow_Count {
    public static void main(String[] args) throws Exception {

        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2. 读取端口数据创建流并转换每一行数据为JavaBean对象
        SingleOutputStreamOperator<WaterSensor1> waterSensor1DS = env.socketTextStream("hadoop106", 9999)
                .map(line -> {
                    String[] split = line.split(",");
                    return new WaterSensor1(split[0],
                            Long.parseLong(split[1]),
                            Integer.parseInt(split[2]));
                });

        // 3. 将流转换为表并指定处理时间
        Table table = tableEnv.fromDataStream(waterSensor1DS,
                $("id"),
                $("ts"),
                $("vc"),
                $("pt").proctime());

        // 4. 开会滚动窗口(计数)计算WordCount
        Table result = table.window(Tumble.over(rowInterval(5L)).on($("pt")).as("cw"))
                .groupBy($("id"), $("cw"))
                .select($("id"), $("id").count());

        // 5. 将结果表转换为流进行输出
        tableEnv.toAppendStream(result, Row.class).print();

        // 6. 执行任务
        env.execute();

    }
}
