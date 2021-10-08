package com.atguigu.day07;

import com.atguigu.bean.ItemCount;
import com.atguigu.bean.ItemCount1;
import com.atguigu.bean.UserBehavior1;
import com.atguigu.bean.UserVisitorCount1;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;

public class Flink05_Practice_ItemCountTopN {

    public static void main(String[] args) throws Exception {

        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

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

        // 4. 按照商品ID分组
        KeyedStream<Tuple2<Long, Integer>, Long> keyedStream = userBehavior1DS.map(new MapFunction<UserBehavior1, Tuple2<Long, Integer>>() {
            @Override
            public Tuple2<Long, Integer> map(UserBehavior1 value) throws Exception {
                return new Tuple2<>(value.getItemId(), 1);
            }
        }).keyBy(data -> data.f0);

        // 5. 开窗计算结果
        SingleOutputStreamOperator<ItemCount1> aggregate = keyedStream.window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
                .aggregate(new ItemCountAggFunc(), new ItemCountWindowFunc());

        // 6. 按照窗口信息重新分组，使用状态编程的方式，实现窗口内TopN
        SingleOutputStreamOperator<String> result = aggregate.keyBy(ItemCount1::getTime)
                .process(new ItemCountProcessFunc(5));


        // 7. 打印并执行任务
        result.print();
        env.execute();

    }

    public static class ItemCountAggFunc implements AggregateFunction<Tuple2<Long, Integer>, Integer, Integer> {
        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(Tuple2<Long, Integer> value, Integer accumulator) {
            return accumulator + 1;
        }

        @Override
        public Integer getResult(Integer accumulator) {
            return accumulator;
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            return a + b;
        }
    }

    public static class ItemCountWindowFunc implements WindowFunction<Integer, ItemCount1, Long, TimeWindow> {
        @Override
        public void apply(Long itemId, TimeWindow window, Iterable<Integer> input, Collector<ItemCount1> out) throws Exception {

            // 取出数据
            Integer count = input.iterator().next();

            // 输出数据
            out.collect(new ItemCount1(itemId
                    ,new Timestamp(window.getEnd()).toString()
                    ,count));

        }
    }

    public static  class ItemCountProcessFunc extends KeyedProcessFunction<String, ItemCount1, String> {

        // 声明状态
        private ListState<ItemCount1> listState;

        private Integer topSize;

        public ItemCountProcessFunc(Integer topSize) {
            this.topSize = topSize;
        }

        @Override
        public void open(Configuration parameters) throws Exception {

            listState = getRuntimeContext().getListState(new ListStateDescriptor<ItemCount1>("list-state", ItemCount1.class));

        }

        @Override
        public void processElement(ItemCount1 value, Context ctx, Collector<String> out) throws Exception {

            // 将数据存入状态
            listState.add(value);

            // 定义定时器
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            ctx.timerService().registerEventTimeTimer(sdf.parse(value.getTime()).getTime() + 1000L);


        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

            // 取出状态中的所有数据
            Iterator<ItemCount1> iterator = listState.get().iterator();

            // 将迭代器转换为集合
            ArrayList<ItemCount1> itemCount1s = Lists.newArrayList(iterator);

            // 按照点击次数排序
            itemCount1s.sort(((o1 , o2) -> o2.getCount() - o1.getCount()));

            // 输出TopSize条数据
            StringBuilder sb = new StringBuilder();
            sb
                    .append("=================")
                    .append(new Timestamp(timestamp-1000L))
                    .append("=================")
                    .append("\n");
            for (int i = 0; i < Math.min(topSize, itemCount1s.size()); i++) {

                ItemCount1 itemCount1 = itemCount1s.get(i);

                sb.append("Top").append(i + 1);
                sb.append(" ItemId:").append(itemCount1.getItem());
                sb.append(" Count:").append(itemCount1.getCount());
                sb.append("\n");

            }

            sb
                    .append("=================")
                    .append(new Timestamp(timestamp-1000L))
                    .append("=================")
                    .append("\n")
                    .append("\n");

            // 清空状态，并输出数据
            listState.clear();
            out.collect(sb.toString());

            // 休息一会
            Thread.sleep(2000);

        }
    }


}
