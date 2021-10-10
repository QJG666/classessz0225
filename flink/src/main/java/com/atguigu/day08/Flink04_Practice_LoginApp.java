package com.atguigu.day08;

import com.atguigu.bean.LoginEvent1;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;

public class Flink04_Practice_LoginApp {
    public static void main(String[] args) throws Exception {

        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 读取文本数据，转换为JavaBean，提取时间戳生成watermark
        WatermarkStrategy<LoginEvent1> loginEvent1WatermarkStrategy = WatermarkStrategy.<LoginEvent1>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner(new SerializableTimestampAssigner<LoginEvent1>() {
                    @Override
                    public long extractTimestamp(LoginEvent1 element, long recordTimestamp) {
                        return element.getEventTime() * 1000L;
                    }
                });

        SingleOutputStreamOperator<LoginEvent1> loginEvent1DS = env.readTextFile("input_flink/LoginLog.csv")
                .map(data -> {
                    String[] split = data.split(",");
                    return new LoginEvent1(Long.parseLong(split[0]),
                            split[1],
                            split[2],
                            Long.parseLong(split[3]));
                }).assignTimestampsAndWatermarks(loginEvent1WatermarkStrategy);

        // 3. 按照用户ID分组
        KeyedStream<LoginEvent1, Long> keyedStream = loginEvent1DS.keyBy(LoginEvent1::getUserId);

        // 4. 使用ProcessAPI，状态+定时器
        SingleOutputStreamOperator<String> result = keyedStream.process(new LoginKeyedProcessFunc(2, 2));

        // 5. 打印结果
        result.print();

        // 6. 执行任务
        env.execute();

    }

    public static class LoginKeyedProcessFunc extends KeyedProcessFunction<Long, LoginEvent1, String> {

        // 定义属性信息
        private Integer ts;
        private Integer count;

        public LoginKeyedProcessFunc(Integer ts, Integer count) {
            this.ts = ts;
            this.count = count;
        }

        // 声明状态
        private ListState<LoginEvent1> loginEvent1ListState;
        private ValueState<Long> valueState;

        @Override
        public void open(Configuration parameters) throws Exception {
            loginEvent1ListState = getRuntimeContext().getListState(new ListStateDescriptor<LoginEvent1>("list-state", LoginEvent1.class));
            valueState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("tsState", Long.class));
        }

        @Override
        public void processElement(LoginEvent1 value, Context ctx, Collector<String> out) throws Exception {

            // 取出状态中的数据
            Iterator<LoginEvent1> iterator = loginEvent1ListState.get().iterator();
            Long timerTs = valueState.value();

            // 取出事件类型
            String eventType = value.getEventType();

            // 判断是否为第一条失败数据，则需要注册定时器
            if ("fail".equals(eventType)) {

                if (!iterator.hasNext()) { // 为第一条失败数据

                    // 注册定时器
                    long curTs = ctx.timerService().currentWatermark() + ts * 1000L;
                    ctx.timerService().registerEventTimeTimer(curTs);

                    // 更新时间状态
                    valueState.update(curTs);

                }

                // 将当前的失败数据加入状态
                loginEvent1ListState.add(value);

            } else {

                // 说明已经注册过定时器
                if (timerTs != null) {

                    ctx.timerService().deleteEventTimeTimer(timerTs);

                }

                // 成功数据，清空List并删除定时器
                loginEvent1ListState.clear();
                valueState.clear();

            }

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

            // 取出状态中的数据
            Iterator<LoginEvent1> iterator = loginEvent1ListState.get().iterator();
            ArrayList<LoginEvent1> loginEvent1s = Lists.newArrayList(iterator);

            // 判断连续失败的次数
            if (loginEvent1s.size() >= count) {
                LoginEvent1 first = loginEvent1s.get(0);
                LoginEvent1 last = loginEvent1s.get(loginEvent1s.size() - 1);

                out.collect(first.getUserId() + "用户在" + first.getEventTime() + "到" + last.getEventTime() + "之间，连续登录失败了" + loginEvent1s.size() + "次");
            }

            // 清空状态
            loginEvent1ListState.clear();
            valueState.clear();

        }
    }
}
