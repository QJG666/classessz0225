package com.atguigu.day02;

import com.atguigu.bean.WaterSensor1;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import scala.compat.java8.converterImpl.RichMapCanStep;

public class Flink08_Transform_RichMap {
    public static void main(String[] args) throws Exception {

        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // 2. 从文件读取数据
        DataStreamSource<String> stringDataStreamSource = env.readTextFile("input/sensor.txt");

        // 3. 将每行数据转换为JavaBean
        SingleOutputStreamOperator<WaterSensor1> waterSensor1DS = stringDataStreamSource.map(new MyRichMapFunc());

        // 4. 打印结果数据
        waterSensor1DS.print();

        // 5. 执行任务
        env.execute();


    }

    // RichFunction富有的地方在于：1. 生命周期方法，2. 可以获取上下文执行环境，做状态编程
    public static class MyRichMapFunc extends RichMapFunction<String, WaterSensor1> {

        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("Open方法被调用!!");;
        }

        @Override
        public WaterSensor1 map(String value) throws Exception {
            String[] split = value.split(",");
            return new WaterSensor1(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
        }

        @Override
        public void close() throws Exception {
            System.out.println("Close方法被调用!!");;
        }
    }
}
