package net.acan.net.acan.flink.chapter07.state;

import net.acan.net.acan.flink.bean.WaterSensor;
import net.acan.net.acan.flink.util.MyUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Comparator;
import java.util.List;

public class KeyedState03_Reduce {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        
        env
                .socketTextStream("hadoop162", 9999)
                .map((MapFunction<String, WaterSensor>) value -> {
                    String[] data = value.split(",");
                    return new WaterSensor(
                            data[0],
                            Long.valueOf(data[1]),
                            Integer.valueOf(data[2])
                    );
                })
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {

                    private ReducingState<Integer> sumVc;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                       sumVc = getRuntimeContext().getReducingState(new ReducingStateDescriptor<Integer>(
                               "sumVc",
                               new ReduceFunction<Integer>() {
                                   @Override
                                   public Integer reduce(Integer value1, Integer value2) throws Exception {
                                       return value1+value2;
                                   }
                               }
                               ,Integer.class
                       ));
                    }

                    @Override
                    public void processElement(WaterSensor value,
                                               Context ctx,
                                               Collector<String> out) throws Exception {
                        sumVc.add(value.getVc());
                        out.collect(ctx.getCurrentKey()+"和"+sumVc.get());
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
