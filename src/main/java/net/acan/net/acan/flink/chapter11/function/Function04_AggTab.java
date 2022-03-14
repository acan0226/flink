package net.acan.net.acan.flink.chapter11.function;

import net.acan.net.acan.flink.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class Function04_AggTab {
    //table Functions 制表函数  一进多出，对应hive中的UDTF-
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        DataStreamSource<WaterSensor> stream = environment.fromElements(
                new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("sensor_2", 3000L, 30),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_1", 5000L, 50),
                new WaterSensor("sensor_2", 6000L, 60)
        );


        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(environment);

        // 3. 把流转成一个表(动态表)
        Table table = tableEnv.fromDataStream(stream);
        //table.printSchema();
        tableEnv.createTemporaryView("sensor",table);

        // 使用自定义函数
        // 1. 在table api中使用
        // 1.1 内联的方式使用
        table
                .groupBy($("id"))
                .flatAggregate(call(Top2.class, $("vc")))
                .select($("id"), $("rank"), $("vc"))
                .execute()
                .print();


        // 1.2 先注册后使用
        //先注册
       // tableEnv.createTemporaryFunction("my_avg", MyAgg.class);


        // 2. 在sql中使用
        // 在sql中无法直接使用

    }
    public static class Top2 extends TableAggregateFunction<Result,Ranks>{

        //创建累加器
        @Override
        public Ranks createAccumulator() {
            return new Ranks();
        }
        // 实现数据累加: 计算出来top2的水位, 存储到累加器中
        // 返回值必须是void 方法名必须是:accumulate 第一个参数: 必须是累加器 后面的参数传递到这里的值
        public void accumulate(Ranks rs ,Integer vc){
            if (vc > rs.first) {
                rs.second = rs.first;
                rs.first = vc;
            }else if (vc > rs.second){
                rs.second = vc;
            }

        }
        // 方法名必须是:emitValue 参数1: 必须是累加器 参数2: Collector<Result> 泛型是你结果类型
        public void emitValue(Ranks rs, Collector<Result> out){
            //第一行
            out.collect(new Result("第一名",rs.first));
            //第二行
            if (rs.second > 0){

            out.collect(new Result("第二名",rs.second));
            }

        }
    }

    public static class Result {
        public String rank;
        public Integer vc;

        public Result(String rank, Integer vc) {
            this.rank = rank;
            this.vc = vc;
        }

        public Result() {
        }
    }

    public static class Ranks {
        public Integer first = 0;
        public Integer second = 0;
    }
}
/*
每来一条数据, 输出水位中的top2
new WaterSensor("sensor_1", 1000L, 10),
                            名次    值
                            第一名   10
new WaterSensor("sensor_1", 2000L, 20),
                           名次    值
                           第一名   20
                           第二名   10

 new WaterSensor("sensor_1", 4000L, 40),
                           名次    值
                           第一名   40
                           第二名   20

                           ....

 */