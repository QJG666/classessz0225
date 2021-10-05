package com.atguigu.day07;

import com.atguigu.bean.PageViewCount1;
import com.atguigu.bean.UserBehavior1;
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
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.Random;

public class Flink07_Practice_PageView_Window2 {

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

        // 4. 将数据转换为元组
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = userBehavior1DS.map(new MapFunction<UserBehavior1, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(UserBehavior1 value) throws Exception {
                return new Tuple2<>("PV_" + new Random().nextInt(12), 1);
            }
        }).keyBy(data -> data.f0);

        // 5. 开窗并计算
        SingleOutputStreamOperator<PageViewCount1> aggResult = keyedStream.window(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(new PageViewAggFunc(), new PageViewWindowFunc());

        // 6. 按照窗口信息重新分组做第二次聚合
        KeyedStream<PageViewCount1, String> pageViewCount1KeyedStream = aggResult.keyBy(PageViewCount1::getTime);

        // 7. 累加结果
        // 7.1 方案一，一个窗口的数据输出多次
        // SingleOutputStreamOperator<PageViewCount1> result = pageViewCount1KeyedStream.sum("count");
        // 7.2 方案二，使用状态编程，一个窗口输出一次
        SingleOutputStreamOperator<PageViewCount1> result = pageViewCount1KeyedStream.process(new PageViewProcessFunc());

        // 8. 打印结果
        result.print();

        // 9. 执行任务
        env.execute();

    }

    public static class PageViewAggFunc implements AggregateFunction<Tuple2<String, Integer>, Integer, Integer> {
        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(Tuple2<String, Integer> value, Integer accumulator) {
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

    public static class PageViewWindowFunc implements WindowFunction<Integer, PageViewCount1, String, TimeWindow> {
        @Override
        public void apply(String key, TimeWindow window, Iterable<Integer> input, Collector<PageViewCount1> out) throws Exception {

            // 提取窗口时间
            String timestamp = new Timestamp(window.getEnd()).toString();

            // 获取累计结果
            Integer count = input.iterator().next();

            // 输出结果
            out.collect(new PageViewCount1("PV", timestamp, count));

        }
    }

    public static class PageViewProcessFunc extends KeyedProcessFunction<String, PageViewCount1, PageViewCount1> {

        // 定义状态
        private ListState<PageViewCount1> listState;

        @Override
        public void open(Configuration parameters) throws Exception {

            listState = getRuntimeContext().getListState(new ListStateDescriptor<PageViewCount1>("list-state", PageViewCount1.class));

        }

        @Override
        public void processElement(PageViewCount1 value, Context ctx, Collector<PageViewCount1> out) throws Exception {

            // 将数据放入状态
            listState.add(value);

            // 注册定时器
            String time = value.getTime();
            long ts = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(time).getTime();
            ctx.timerService().registerEventTimeTimer(ts+1);

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<PageViewCount1> out) throws Exception {

            // 取出状态中的数据
            Iterable<PageViewCount1> pageViewCount1s = listState.get();

            // 遍历累加数据
            Integer count = 0;
            Iterator<PageViewCount1> iterator = pageViewCount1s.iterator();
            while (iterator.hasNext()) {
                PageViewCount1 next = iterator.next();
                count += next.getCount();
            }

            // 输出数据
            out.collect(new PageViewCount1("PV", new Timestamp(timestamp-1).toString(), count));

            // 清空状态
            listState.clear();

        }
    }


}
