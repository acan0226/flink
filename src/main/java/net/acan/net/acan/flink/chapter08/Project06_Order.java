package net.acan.net.acan.flink.chapter08;

import net.acan.net.acan.flink.bean.LoginEvent;
import net.acan.net.acan.flink.bean.OrderEvent;
import net.acan.net.acan.flink.util.MyUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;

public class Project06_Order {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

env.readTextFile("input/OrderLog.csv")
        .map(line->{
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
        .keyBy(OrderEvent::getOrderId)
        .window(EventTimeSessionWindows.withGap(Time.minutes(30L)))
        .process(new ProcessWindowFunction<OrderEvent, String, Long, TimeWindow>() {

            private ValueState<OrderEvent> orderState;

            @Override
            public void open(Configuration parameters) throws Exception {
                orderState = getRuntimeContext().getState(new ValueStateDescriptor<OrderEvent>("orderState", OrderEvent.class));
            }

            @Override
            public void process(Long orderId,
                                Context ctx,
                                Iterable<OrderEvent> elements,
                                Collector<String> out) throws Exception {
                List<OrderEvent> list = MyUtil.toList(elements);

                if (list.size() == 2){// 窗口内两条元素: create和pay都在, 正常支付
                    out.collect(orderId+":正常支付");
                }else{
                    //如果只有一条，那就是有异常
                    OrderEvent orderEvent = list.get(0);
                    String eventType = orderEvent.getEventType();

                    if ("create".equals(eventType)) {
                        //create放入状态中
                        orderState.update(orderEvent);
                    }else{
                        if (orderState.value() != null) {
                            out.collect(orderId+":30分钟未支付");
                        }else{
                            //没create有pay
                            out.collect(orderId+":未创建订单并支付");
                        }
                    }
                }
            }
        })
        .print();



    try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
