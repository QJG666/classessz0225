package com.atguigu.day03;

import com.alibaba.fastjson.JSON;
import com.atguigu.bean.WaterSensor1;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

public class Flink10_Sink_MySink {
    public static void main(String[] args) throws Exception {

        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 读取端口数据并转换为javabean
        SingleOutputStreamOperator<WaterSensor1> waterSensorDS = env.socketTextStream("hadoop106", 9999)
                .map(new MapFunction<String, WaterSensor1>() {
                    @Override
                    public WaterSensor1 map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new WaterSensor1(split[0],
                                Long.parseLong(split[1]),
                                Integer.parseInt(split[2]));
                    }
                });


        // 3. 将数据写入MySQL
        waterSensorDS.addSink(new MySink());


        // 4. 执行任务
        env.execute();


    }

    public static class MySink extends RichSinkFunction<WaterSensor1> {

        // 声明连接
        private Connection connection;
        private PreparedStatement preparedStatement;

        // 生命周期方法，用于创建连接
        @Override
        public void open(Configuration parameters) throws Exception {
            connection = DriverManager.getConnection("jdbc:mysql://hadoop106:3306/test?useSSL=false",
                    "root", "123456");
            preparedStatement = connection.prepareStatement(
                    "insert into `sensor` values(?, ?, ?) on DUPLICATE KEY UPDATE `ts`=?,`vc`=?");
        }


        @Override
        public void invoke(WaterSensor1 value, Context context) throws Exception {

            // 给占位符赋值
            preparedStatement.setString(1, value.getId());
            preparedStatement.setLong(2, value.getTs());
            preparedStatement.setInt(3, value.getVc());
            preparedStatement.setLong(4, value.getTs());
            preparedStatement.setInt(5, value.getVc());

            // 执行操作
            preparedStatement.execute();

        }

        //生命周期方法，用于关闭连接
        @Override
        public void close() throws Exception {
            preparedStatement.close();
            connection.close();
        }
    }
}
