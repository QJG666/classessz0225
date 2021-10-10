package com.atguigu.day08;

import com.atguigu.bean.AdCount1;
import com.atguigu.bean.AdsClickLog1;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Timestamp;

public class Flink03_Practice_AdCount {
    public static void main(String[] args) throws Exception {

        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 读取文本数据创建流，转换为JavaBean，提取时间戳生成WaterMark
        WatermarkStrategy<AdsClickLog1> adsClickLog1WatermarkStrategy = WatermarkStrategy.<AdsClickLog1>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<AdsClickLog1>() {
                    @Override
                    public long extractTimestamp(AdsClickLog1 element, long recordTimestamp) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        SingleOutputStreamOperator<AdsClickLog1> adsClickLog1DS = env.readTextFile("input_flink/AdClickLog.csv")
                .map(data -> {
                    String[] split = data.split(",");
                    return new AdsClickLog1(Long.parseLong(split[0]),
                            Long.parseLong(split[1]),
                            split[2],
                            split[3],
                            Long.parseLong(split[4]));
                }).assignTimestampsAndWatermarks(adsClickLog1WatermarkStrategy);

        // 根据黑名单做过滤
        SingleOutputStreamOperator<AdsClickLog1> filterDS = adsClickLog1DS
                .keyBy(data->data.getUserId() + "_" + data.getAdId())
                .process(new BlackListProcessFunc(100L));

        // 3. 按照省份分组
        KeyedStream<AdsClickLog1, String> provinceKeyedStream = filterDS.keyBy(AdsClickLog1::getProvince);

        // 4. 开窗聚合，增量聚合+窗口函数补充窗口信息
        SingleOutputStreamOperator<AdCount1> result = provinceKeyedStream.window(SlidingEventTimeWindows.of(Time.hours(1), Time.seconds(5)))
                .aggregate(new AdCountAggFunc(), new AdCountWindowFunc());

        // 5. 打印结果
        result.print();
        filterDS.getSideOutput(new OutputTag<String>("BlackList"){
        }).print("SideOutPut");

        // 6. 执行任务
        env.execute();

    }

    public static class AdCountAggFunc implements AggregateFunction<AdsClickLog1, Integer, Integer> {
        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(AdsClickLog1 value, Integer accumulator) {
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

    public static class AdCountWindowFunc implements WindowFunction<Integer, AdCount1, String, TimeWindow> {
        @Override
        public void apply(String province, TimeWindow window, Iterable<Integer> input, Collector<AdCount1> out) throws Exception {

            Integer count = input.iterator().next();
            out.collect(new AdCount1(province, window.getEnd(), count));

        }
    }

    public static class BlackListProcessFunc extends KeyedProcessFunction<String, AdsClickLog1, AdsClickLog1> {

        // 定义最大的点击次数属性
        private Long maxClickCount;

        // 声明状态
        private ValueState<Long> countState;
        private ValueState<Boolean> isSendState;

        public BlackListProcessFunc(Long maxClickCount) {
            this.maxClickCount = maxClickCount;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("count-state", Long.class));
            isSendState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("is-send-state", Boolean.class));
        }

        @Override
        public void processElement(AdsClickLog1 value, Context ctx, Collector<AdsClickLog1> out) throws Exception {

            // 取出状态中的数据
            Long count = countState.value();
            Boolean isSend = isSendState.value();

            // 判断是否是第一条数据
            if (count == null) {

                // 赋值为1
                countState.update(1L);

                // 注册以第二天凌晨的定时器，用于清空状态
                long ts = value.getTimestamp() / ((60 * 60 * 24) + 1) * (24 * 60 * 60 * 1000L) - 8 * 60 * 60 * 1000L;
                // System.out.println(new Timestamp(ts));
                ctx.timerService().registerProcessingTimeTimer(ts);

            } else {

                // 非第一条数据
                count = count + 1;

                // 更新状态
                countState.update(count);

                // 判断是否超过阈值
                if (count >= maxClickCount) {

                    if (isSend == null) {

                        // System.out.println("报警信息");

                        // 报警信息进侧输出流
                        ctx.output(new OutputTag<String>("BlackList"){
                            },
                                value.getUserId() + "点击了" + value.getAdId() + "广告达到" + maxClickCount + "次，存在恶意点击广告行为，报警!" );

                        // 更新黑名单状态
                        isSendState.update(true);
                    }

                    return;

                }

            }

            // 输出数据
            out.collect(value);

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<AdsClickLog1> out) throws Exception {
            isSendState.clear();
            countState.clear();
        }
    }

}
