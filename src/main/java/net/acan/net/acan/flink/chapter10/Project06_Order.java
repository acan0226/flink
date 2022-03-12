package net.acan.net.acan.flink.chapter10;

import net.acan.net.acan.flink.bean.OrderEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.*;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.api.windowing.time.Time;

import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


import java.time.Duration;
import java.util.List;
import java.util.Map;

public class Project06_Order {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        KeyedStream<OrderEvent, Long> stream = env.readTextFile("input/OrderLog.csv")
                .map(line -> {
                    String[] data = line.split(",");
                    return new OrderEvent(
                            Long.valueOf(data[0]),
                            data[1],
                            data[2],
                            Long.valueOf(data[3]) * 1000
                    );
                }).assignTimestampsAndWatermarks(WatermarkStrategy
                        .<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((SerializableTimestampAssigner<OrderEvent>) (element, recordTimestamp) -> element.getEventTime())
                )
                .keyBy(OrderEvent::getOrderId);
            //定义模式(规则)：有create和pay
        Pattern<OrderEvent, OrderEvent> pattern = Pattern
                .<OrderEvent>begin("create", AfterMatchSkipStrategy.skipPastLastEvent())
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return "create".equals(value.getEventType());
                    }
                })
                .optional()
                .next("pay")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return "pay".equals(value.getEventType());
                    }
                })
                .within(Time.minutes(30));
        // 假设pay来晚了, 或者没有pay, 则create是超时数据
            //把规则作用在流上，得到模式流
        PatternStream<OrderEvent> ps = CEP.pattern(stream, pattern);

        //将模式流中的数据输出

        // 假设pay来晚了, 或者没有pay, 则create金融局超时数据
//        SingleOutputStreamOperator<String> normal = ps.flatSelect(
//                new OutputTag<OrderEvent>("timeout") {
//                },
//                new PatternFlatTimeoutFunction<OrderEvent, String>() {
//                    @Override
//                    public void timeout(Map<String, List<OrderEvent>> pattern,
//                                        long timeoutTimestamp,
//                                        Collector<String> out) throws Exception {
//                        OrderEvent create = pattern.get("create").get(0);
//                        String msg = "订单:" + create.getOrderId() + " 没有pay或者pay超时支付...";
//                        out.collect(msg);
//
//                    }
//                },
//                 new PatternFlatSelectFunction<OrderEvent, String>() {
//                    @Override
//                    public void flatSelect(Map<String,
//                            List<OrderEvent>> map,
//                                           Collector<String> out) throws Exception {
//                        //正常的流因为加了optional,有两种情况:一种是有create和pay,一种是只有pay
//                        // 1.既有create又有pay是正常的, 放弃
//                        // 2.只取只有pay
//                        if (!map.containsKey("create")) {
//                            OrderEvent pay = map.get("pay").get(0);
//                            String msg = "用户" + pay.getOrderId() + "只有pay没有create";
//                            out.collect(msg);
//                        }
//                    }
//                }
//        );


        //将模式流中的数据输出
        // 假设pay来晚了, 或者没有pay, 则create是超时数据
        SingleOutputStreamOperator<String> normal = ps.flatSelect(
                new OutputTag<String>("timeout") {
                },
                new PatternFlatTimeoutFunction<OrderEvent, String>() {
                    @Override
                    public void timeout(Map<String, List<OrderEvent>> pattern,
                                        long timeoutTimestamp,
                                        Collector<String> out) throws Exception {
                        OrderEvent create = pattern.get("create").get(0);
                        String msg = "订单:" + create.getOrderId() + " 没有pay或者pay超时支付...";
                        out.collect(msg);

                    }
                },
                new PatternFlatSelectFunction<OrderEvent, String>() {

                    @Override
                    public void flatSelect(Map<String, List<OrderEvent>> map,
                                           Collector<String> out) throws Exception {

                        //正常的流因为加了optional,有两种情况:一种是有create和pay,一种是只有pay
                        //既有create又有pay是正常的, 放弃
                        // 只取只有pay
                        if (!map.containsKey("create")) {
                            OrderEvent pay = map.get("pay").get(0);
                            String msg = "订单:" + pay.getOrderId() + " 只有pay没有create, 请检查系统bug";
                            out.collect(msg);
                        }
                    }
                }
        );

        normal.print("正常");
        normal.getSideOutput(new OutputTag<String>("timeout"){
        }).print("异常");

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
