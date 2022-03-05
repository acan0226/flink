package net.acan.net.acan.flink.chapter06;

import net.acan.net.acan.flink.bean.OrderEvent;
import net.acan.net.acan.flink.bean.TxEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;

public class Flink04_Order {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        SingleOutputStreamOperator<OrderEvent> oeStream = env.readTextFile("input/OrderLog.csv")
                .map(new MapFunction<String, OrderEvent>() {
                    @Override
                    public OrderEvent map(String s) throws Exception {
                        String[] data = s.split(",");
                        return new OrderEvent(
                                Long.valueOf(data[0]),
                                data[1],
                                data[2],
                                Long.valueOf(data[3])
                        );
                    }
                })
                .filter(a -> "pay".equals(a.getEventType()));

        SingleOutputStreamOperator<TxEvent> rcStream = env.readTextFile("input/ReceiptLog.csv")
                .map(new MapFunction<String, TxEvent>() {
                    @Override
                    public TxEvent map(String s) throws Exception {
                        String[] data = s.split(",");
                        return new TxEvent(
                                data[0],
                                data[1],
                                Long.valueOf(data[2])
                        );
                    }
                });

        oeStream.connect(rcStream)
                .keyBy(OrderEvent::getTxId, TxEvent::getTxId)
                .process(new CoProcessFunction<OrderEvent, TxEvent, String>() {
                    HashMap<String, OrderEvent> txIdToOrderEvent = new HashMap<>();
                    HashMap<String, TxEvent> txIdToTxEvent = new HashMap<>();
                    @Override
                    public void processElement1(OrderEvent value,
                                                        Context ctx,
                                                Collector<String> out) throws Exception {
                        String txId = value.getTxId();
                        if(txIdToTxEvent.containsKey(txId)){
                            out.collect(value.getOrderId()+"对账成功");
                        }else{
                            txIdToOrderEvent.put(txId, value);
                        }
                    }

                    @Override
                    public void processElement2(TxEvent value,
                                               Context ctx,
                                                Collector<String> out) throws Exception {
                        String txId = value.getTxId();
                        if (txIdToOrderEvent.containsKey(txId)) {
                            out.collect("订单: " + txIdToOrderEvent.get(txId).getOrderId() + " 对账成功!!!");
                        } else {
                            txIdToTxEvent.put(txId, value);
                        }
                    }
                }).print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
