package net.acan.net.acan.flink.chapter07.state;

import net.acan.net.acan.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class KeyedState01_Value {
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
                .flatMap(new RichFlatMapFunction<WaterSensor, String>() {

                    private ValueState<Integer> lastVcState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        lastVcState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("lastVcState", Integer.class));
                    }

                    @Override
                    public void flatMap(WaterSensor value,
                                        Collector<String> out) throws Exception {
                        // 如果是第一条数据, 则把水位存到状态中, 如果不是第一条, 则把状态中的水位值取出
                        if (lastVcState.value() == null) {
                            lastVcState.update(value.getVc());
                        }else {
                            //不是第一条
                            Integer lastVc = lastVcState.value();
                            if (lastVc > 10 && value.getVc()>10) {
                                out.collect(value.getId() + " 连续两次水位超过10, 发出橙色预警...");
                            }
                            //再次将本次值放入状态中,作为lastVc,为下次的水位使用
                            lastVcState.update(value.getVc());
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
