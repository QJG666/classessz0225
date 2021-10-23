package com.atguigu.day10;

import com.atguigu.bean.WaterSensor;
import com.atguigu.bean.WaterSensor1;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.kafka.clients.producer.ProducerConfig;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkSQL07_Sink_Kafka1 {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<WaterSensor1> waterSensorStream =
                env.fromElements(new WaterSensor1("sensor_1", 1000L, 10),
                        new WaterSensor1("sensor_1", 2000L, 20),
                        new WaterSensor1("sensor_2", 3000L, 30),
                        new WaterSensor1("sensor_1", 4000L, 40),
                        new WaterSensor1("sensor_1", 5000L, 50),
                        new WaterSensor1("sensor_2", 6000L, 60));
        // 1. 创建表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Table sensorTable = tableEnv.fromDataStream(waterSensorStream);
        Table resultTable = sensorTable
                .where($("id").isEqual("sensor_1"))
                .select($("id"), $("ts"), $("vc"));

        // 创建输出表
        Schema schema = new Schema()
                .field("id", DataTypes.STRING())
                .field("ts", DataTypes.BIGINT())
                .field("vc", DataTypes.INT());
        tableEnv
                .connect(new Kafka()
                        .version("universal")
                        .topic("test")
                        .sinkPartitionerRoundRobin()
//                        .property("bootstrap.servers", "hadoop105:9092,hadoop106:9092,hadoop107:9092"))
//                        .property(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop105:9092,hadoop106:9092,hadoop107:9092"))
                        .property(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop106:9092"))
                .withFormat(new Json())
                .withSchema(schema)
                .createTemporaryTable("sensor");

        // 把数据写入到输出表中
        resultTable.executeInsert("sensor");

        env.execute();


    }
}
