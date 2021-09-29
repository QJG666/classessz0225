package com.atguigu.day05;

import com.atguigu.bean.WaterSensor1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class Flink08_Process_SideOutPut {
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

        // 3. 使用ProcessFunction将数据分流
        SingleOutputStreamOperator<WaterSensor1> result = waterSensor1DS.process(new SplitProcessFunc());

        // 4. 打印数据
        result.print("主流");
        DataStream<Tuple2<String, Integer>> sideOutput = result
                .getSideOutput(new OutputTag<Tuple2<String, Integer>>("SideOut") {
                });
        sideOutput
                .print("Side");

        // 5. 执行任务
        env.execute();



    }

    public static class SplitProcessFunc extends ProcessFunction<WaterSensor1, WaterSensor1>{
        @Override
        public void processElement(WaterSensor1 value, Context ctx, Collector<WaterSensor1> out) throws Exception {

            // 取出水位线
            Integer vc = value.getVc();

            // 根据水位线高低分流
            if (vc >= 30) {
                // 将数据输出至主流
                out.collect(value);
            } else {
                // 将数据输出至侧输出流
                ctx.output(new OutputTag<Tuple2<String, Integer>>("SideOut"){}, new Tuple2<>(value.getId(), vc));
            }

        }
    }
}
