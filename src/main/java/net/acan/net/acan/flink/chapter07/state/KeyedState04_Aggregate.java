package net.acan.net.acan.flink.chapter07.state;

import net.acan.net.acan.flink.bean.WaterSensor;
import net.acan.net.acan.flink.util.MyUtil;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;


import java.util.Comparator;
import java.util.List;

public class KeyedState04_Aggregate {
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


                    private AggregatingState<WaterSensor, Double> avgVc;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        avgVc = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<WaterSensor, Avg, Double>(
                                "avgVc",
                                new AggregateFunction<WaterSensor, Avg, Double>() {
                                    @Override
                                    public Avg createAccumulator() {
                                        return new Avg();
                                    }

                                    @Override
                                    public Avg add(WaterSensor value, Avg accumulator) {
                                        accumulator.sum += value.getVc();
                                        accumulator.count++;
                                        return accumulator;
                                    }

                                    @Override
                                    public Double getResult(Avg accumulator) {
                                        return accumulator.avg();
                                    }

                                    @Override
                                    public Avg merge(Avg a, Avg b) {
                                        return null;
                                    }
                                }
                                , Avg.class
                        ));
                    }

                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        avgVc.add(value);
                        Double avg = avgVc.get();
                        out.collect(ctx.getCurrentKey()+avg);
                    }
                })
                .print();
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static class Avg {
        public Integer sum = 0;
        public Long count = 0L;

        public Double avg() {
            return sum * 1.0 / count;
        }
    }
}
