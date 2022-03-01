package net.acan.net.acan.flink.chapter04;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Wc_UnBounded {
    public static void main(String[] args) throws Exception {
        //1.创建一个流的执行环境
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        env.disableOperatorChaining();
        //通过env得到一个数据流
        DataStreamSource<String> dataStreamSource = env.socketTextStream("hadoop162", 9999);
        //各种转换
        SingleOutputStreamOperator<Tuple2<String, Long>> result = dataStreamSource.flatMap(new FlatMapFunction<String, String>() {

            @Override
            public void flatMap(String line, Collector<String> collector) throws Exception {
                String[] words = line.split(" ");
                for (String word : words) {
                    collector.collect(word);
                }
            }//.startNewChain()  // 开启一个新的链: 当前算子不和前面优化在一起
            //.disableChaining() // 当前算子不和任何其他算子优化在一起
        }).map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String word) throws Exception {
                return Tuple2.of(word, 1L);
            }
        }).keyBy(new KeySelector<Tuple2<String, Long>, String>() {//不会改变数据结构，仅仅是对元素进行分组
            @Override
            public String getKey(Tuple2<String, Long> stringLongTuple2) throws Exception {
                return stringLongTuple2.f0;
            }
        }).sum(1);//对tuple2中的位置1的元素做聚合.

        result.print();
        env.execute(); //执行环境
    }
}
/*
-----
操作链的优化:
1. 算子之间的关系必须是 one-to-one
2. 算子之间的并行度必须一致



4个地方可以设置并行度
低->高  优先级

1. 在配置文件中
    设置的默认并行度
    parallelism.default: 1

2. 提交job的时候 通过参数 -p 指定并行度
    -p 2

3. 在代码中定义并行度
    当前job所有算子的并行度
    env.setParallelism(2);

4. 在算子上设置
    .setParallelism(2)

 */