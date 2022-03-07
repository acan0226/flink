package net.acan.net.acan.flink.chapter07.state;

import net.acan.net.acan.flink.bean.WaterSensor;
import net.acan.net.acan.flink.util.MyUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.List;

public class State09_Beckend {
    public static void main(String[] args) {

        System.setProperty("HADOOP_USER_NAME", "atguigu");

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        env.enableCheckpointing(2000);//开启checkpoint

        //fs  存储在hdfs中磁盘(主要用这种)
        //env.setStateBackend(new HashMapStateBackend());
        //env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop162:8020/ck1");

        //rockDB  rockDB数据库，内存加磁盘
        env.setStateBackend(new EmbeddedRocksDBStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop162:8020/ck2");

        env
                .socketTextStream("hadoop162", 9999)
                .map(new MapFunction<String, WaterSensor>() {

                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] data = value.split(",");
                        return new WaterSensor(
                                data[0],
                                Long.valueOf(data[1]),
                                Integer.valueOf(data[2])
                        );
                    }
                })
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {

                    private MapState<Integer, Object> vcState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        vcState = getRuntimeContext().getMapState(
                                new MapStateDescriptor<Integer, Object>(
                                        "vcState",
                                        Integer.class,
                                        Object.class));
                    }

                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                            vcState.put(value.getVc(), new Object());
                        List<Integer> vcs = MyUtil.toList(vcState.keys());
                        out.collect(ctx.getCurrentKey()+"不重複"+vcs);
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
