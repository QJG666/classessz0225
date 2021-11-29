package com.atguigu.day12;

import com.atguigu.bean.WaterSensor1;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class FlinkSQL05_Function_UDAF {

    public static void main(String[] args) throws Exception {

        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2. 读取端口数据并转换为JavaBean
        SingleOutputStreamOperator<WaterSensor1> waterSensor1DS = env.socketTextStream("hadoop106", 9999)
                .map(line -> {
                    String[] split = line.split(",");
                    return new WaterSensor1(split[0],
                            Long.parseLong(split[1]),
                            Integer.parseInt(split[2]));
                });

        // 3. 将流转换为动态表
        Table table = tableEnv.fromDataStream(waterSensor1DS);

        // 4. 先注册再使用
        tableEnv.createTemporarySystemFunction("myavg", MyAvg.class);

        //TableAPI
        //table.groupBy($("id"))
        //        .select($("id"), call("myavg", $("vc")))
        //        .execute()
        //        .print();

        // SQL
        tableEnv.sqlQuery("select id, myavg(vc) from " + table + " group by id")
                .execute()
                .print();

        // 5. 执行任务
        env.execute();

    }

    public static class MyAvg extends AggregateFunction<Double, SumCount> {

        @Override
        public SumCount createAccumulator() {
            return new SumCount();
        }

        public void accumulate(SumCount acc, Integer vc) {
            acc.setVcSum(acc.getVcSum() + vc);
            acc.setCount(acc.getCount() + 1);
        }

        @Override
        public Double getValue(SumCount accumulator) {
            return accumulator.getVcSum() * 1D / accumulator.getCount();
        }



    }

}
