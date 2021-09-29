package com.atguigu.day03;

import com.atguigu.bean.WaterSensor1;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Flink11_Sink_JDBC {
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
        waterSensorDS.addSink(JdbcSink.sink(
                "insert into `sensor` values(?, ?, ?) on DUPLICATE KEY UPDATE `ts`=?,`vc`=?",

                new JdbcStatementBuilder<WaterSensor1>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, WaterSensor1 waterSensor1) throws SQLException {
                        preparedStatement.setString(1, waterSensor1.getId());
                        preparedStatement.setLong(2, waterSensor1.getTs());
                        preparedStatement.setInt(3, waterSensor1.getVc());
                        preparedStatement.setLong(4, waterSensor1.getTs());
                        preparedStatement.setInt(5, waterSensor1.getVc());
                    }
                },

                JdbcExecutionOptions.builder()
                        .withBatchSize(1)
                        .build(),

                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://hadoop106:3306/test?useSSL=false")
                        .withDriverName("com.mysql.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("123456")
                        .build()
        ));

        // 4. 执行任务
        env.execute();

    }
}
