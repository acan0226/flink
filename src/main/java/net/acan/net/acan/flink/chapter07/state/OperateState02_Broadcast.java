package net.acan.net.acan.flink.chapter07.state;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class OperateState02_Broadcast {
    public static void main(String[] args) {
          /*
        从socket读数据, 存入到一个ArrayList
        如何把ArrayList中的数据存入到列表状态, 并在程序恢复能够从状态把数据再恢复到ArrayList
         */
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);

        /*
        广播状态使用的套路:
        1. 需要两个流: 1个是数据流  2 配置流. -配置流中的数据用来控制数据流中的数据的处理方式
        2. 把配置流做成广播流
        3. 让数据流去connect广播流
        4. 把广播流中的数据放入到广播状态
        5. 数据流中的数据处理的时候, 从广播状态读取配置信息
         */
        DataStreamSource<String> dataStream = env.socketTextStream("hadoop162", 8888);
        DataStreamSource<String> configStream = env.socketTextStream("hadoop162", 9999);

        MapStateDescriptor<String, String> configStateDesc = new MapStateDescriptor<>("configState", String.class, String.class);

        // 2. 把配置流做成广播流
        BroadcastStream<String> bcStream = 
                configStream.broadcast(configStateDesc);
        //3. 让数据流去connect广播流
        dataStream.
                connect(bcStream)
                .process(new BroadcastProcessFunction<String, String, String>() {
                    // 处理数据流的数据
                    @Override
                    public void processElement(String value,
                                               ReadOnlyContext ctx,
                                               Collector<String> out) throws Exception {
                        ReadOnlyBroadcastState<String, String> bcState = ctx.getBroadcastState(configStateDesc);
                        String s = bcState.get("acan");
                        if ("1".equals(s)) {
                            out.collect("对数据: " + value + " 使用 1 号处理逻辑");
                        }else if ("2".equals(s)) {
                            out.collect("对数据: " + value + " 使用 2 号处理逻辑");
                        } else if ("3".equals(s)) {
                            out.collect("对数据: " + value + " 使用 3 号处理逻辑");
                        }else{

                            out.collect("对数据: " + value + " 使用 默认 号处理逻辑");
                        }
                    }
                    // 处理广播流的数据
                    @Override
                    public void processBroadcastElement(String value, Context ctx,
                                                        Collector<String> out) throws Exception {
                        // 4. 把广播流中的数据放入到广播状态
                        BroadcastState<String, String> bcState = ctx.getBroadcastState(configStateDesc);
                        //广播状态的底层是map，k,v，所以用put
                        bcState.put("acan",value);
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
