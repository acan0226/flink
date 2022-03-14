package net.acan.net.acan.flink.chapter11.function;

import net.acan.net.acan.flink.bean.WaterSensor;
import net.acan.net.acan.flink.bean.WordLen;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.rocksdb.Env;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class Function02_Table {
    //table Functions 制表函数  一进多出，对应hive中的UDTF-
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        DataStreamSource<String> stream = environment.fromElements(
                "hello hello atguigu",
                "hello atguigu",
                "zs lisi wangwu");


        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(environment);

        // 3. 把流转成一个表(动态表)
        Table table = tableEnv.fromDataStream(stream,$("line"));
        table.printSchema();
        tableEnv.createTemporaryView("sensor",table);

        // 使用自定义函数
        // 1. 在table api中使用
        // 1.1 内联的方式使用

//        table
//                .joinLateral(call(Split.class, $("line"))) //把line的值传入到Split.class函数中去
//                .select($("line"),$("word"), $("len"))
//                .execute()
//                .print();


        // 1.2 先注册后使用
//        tableEnv.createTemporaryFunction("split", Split.class);
//        table
//              //  .joinLateral(call("split", $("line")))// 默认是内连接
//                .leftOuterJoinLateral(call("split", $("line")))
//                .select($("line"),$("word"), $("len"))
//                .execute()
//                .print();


        // 2. 在sql中使用
        // 先注册
        tableEnv.createTemporaryFunction("split", Split.class);
        tableEnv.sqlQuery(
                "select " +
                        "line, " +
                        "word, " +
                        "len " +
                        "from sensor " +
                        "left outer join lateral table(split(line)) on true"
        )
                .execute()
                .print();

    }
    // Row用来表示制成的表的每行数据的封装 也可以使用POJO
    // Row是一种弱类型, 需要明确的指定字段名和类型
//    @FunctionHint(output = @DataTypeHint("row<word string,len int>"))
//    public static class Split extends TableFunction<Row>{
//        public void eval(String line){
//            // 数组的长度是几, 制成的表就几行
//            String[] words = line.split(" ");
//            for (String word : words) {
//                //of方法传入几个参数, 就表示一行有几列
//                collect(Row.of(word,word.length()));//调用一次就有一行
//            }
//        }
//    }

    // POJO是一种强类型, 每个字段的类型和名字都是和POJO中的属性保持了一致. 不用额外的配置
    public static class Split extends TableFunction<WordLen>{

        public void eval(String line){
            // 一些特殊情况, 这个值不生成表
            if(line.contains("zs")){
            return;
            }
            // 数组的长度是几, 制成的表就几行
            String[] words = line.split(" ");
            for (String word : words) {
                //of方法传入几个参数, 就表示一行有几列
                collect(new WordLen(word,word.length()));//调用一次就有一行
            }
        }

    }


}

/*
 "hello hello atguigu"
                        hello   5
                        hello   5
                        atguigu  7
"hello atguigu"
                        hello 5
                        atguigu 7

....


 */
