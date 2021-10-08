package com.atguigu.day07;

import com.atguigu.bean.UserBehavior1;
import com.atguigu.bean.UserVisitorCount1;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.sql.Timestamp;
import java.util.HashSet;
import java.util.Iterator;

public class Flink04_Practice_UserVisitor_Window2 {

    public static void main(String[] args) throws Exception {

        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 读取文本数据
        DataStreamSource<String> readTextFile = env.readTextFile("input_flink/UserBehavior.csv");

        // 3. 转换为JavaBean，根据行为过滤数据，并提取时间戳生成WaterMark
        WatermarkStrategy<UserBehavior1> userBehavior1WatermarkStrategy = WatermarkStrategy.<UserBehavior1>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior1>() {
                    @Override
                    public long extractTimestamp(UserBehavior1 element, long recordTimestamp) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        SingleOutputStreamOperator<UserBehavior1> userBehavior1DS = readTextFile.map(data -> {
            String[] split = data.split(",");
            return new UserBehavior1(Long.parseLong(split[0]),
                    Long.parseLong(split[1]),
                    Integer.parseInt(split[2]),
                    split[3],
                    Long.parseLong(split[4]));
        }).filter(data -> "pv".equals(data.getBehavior()))
                .assignTimestampsAndWatermarks(userBehavior1WatermarkStrategy);

        // 4. 按照行为分组
        KeyedStream<UserBehavior1, String> keyedStream = userBehavior1DS.keyBy(UserBehavior1::getBehavior);

        // 5. 开窗
        WindowedStream<UserBehavior1, String, TimeWindow> windowedStream = keyedStream.window(TumblingEventTimeWindows.of(Time.hours(1)));

        // 6. 使用布隆过滤器
        // 6.1 自定义触发器: 来一条计算一条(访问Redis一次)
        SingleOutputStreamOperator<UserVisitorCount1> result = windowedStream
                .trigger(new MyTrigger())
                .process(new UserVisitorWindowFunc());

        // 7. 打印并执行任务
        result.print();
        env.execute();

    }

    public static class MyTrigger extends Trigger<UserBehavior1, TimeWindow> {
        @Override
        public TriggerResult onElement(UserBehavior1 element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.FIRE_AND_PURGE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {

        }
    }

    public static class UserVisitorWindowFunc extends ProcessWindowFunction<UserBehavior1, UserVisitorCount1, String, TimeWindow> {

        // 声明Redis连接
        private Jedis jedis;

        // 声明布隆过滤器
        private MyBloomFilter myBloomFilter;

        // 声明每个窗口总人数的Key
        private String hourUVCountKey;

        @Override
        public void open(Configuration parameters) throws Exception {

            jedis = new Jedis("hadoop106", 6379);
            hourUVCountKey = "HourUv";
            myBloomFilter = new MyBloomFilter(1 << 30); // 2的30次方

        }

        @Override
        public void process(String key, Context context, Iterable<UserBehavior1> elements, Collector<UserVisitorCount1> out) throws Exception {

            // 1. 取出数据
            UserBehavior1 userBehavior1 = elements.iterator().next();

            // 2. 提取窗口信息
            String windowEnd = new Timestamp(context.window().getEnd()).toString();

            // 3. 定义当前窗口的BitMap Key
            String bitMapKey = "BitMap_" + windowEnd;

            // 4. 查询当前的UID是否已经存在于当前的bitMap中
            long offset = myBloomFilter.getOffset(userBehavior1.getUserId().toString());
            Boolean exist = jedis.getbit(bitMapKey, offset);

            // 5. 根据数据是否存在决定下一步操作
            if (!exist) {

                // 将对应Offset位置改为1
                jedis.setbit(bitMapKey, offset, true);

                // 累加当前窗口的总和
                jedis.hincrBy(hourUVCountKey, windowEnd, 1);

            }

            // 输出数据
            String hget = jedis.hget(hourUVCountKey, windowEnd);
            out.collect(new UserVisitorCount1("UV", windowEnd, Integer.parseInt(hget)));

        }

    }

    // 自定义布隆过滤器
    public static class MyBloomFilter {

        // 定义布隆过滤器容量，最好传入2的整次幂数据
        // cap -> BitMap
        private long cap;

        public MyBloomFilter(long cap) {
            this.cap = cap;
        }

        // 传入一个字符串，获取在BitMap中的位置信息
        private long getOffset(String value) {

            long result = 0L;

            for (char c :
                    value.toCharArray()) {
                result += result * 31 + c;
            }

            // 取模: 方式一
            // 位与预算，效率更高一些: 方式二
            return result & (cap - 1);

        }

    }

}
