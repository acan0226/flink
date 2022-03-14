package net.acan.net.acan.flink.chapter12;

import net.acan.net.acan.flink.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class TopN {
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        //创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(environment);
                /*
            private Long userId;
            private Long itemId;
            private Integer categoryId;
            private String behavior;
            private Long timestamp;
         */
        //1、读取数据，从文件中
        tableEnv.executeSql("create table ub(" +
                " userId bigint, " +
                " itemId bigint, " +
                " categoryId int, " +
                " behavior string, " +
                " ts bigint , " +
                "et as to_timestamp_ltz(ts,0), " +
                "watermark for et as et - interval '3' second " +
                ")with(" +
                "'connector' = 'filesystem', " +
                "'path' = 'input/UserBehavior.csv', " +
                "'format' = 'csv' " +
                ")");
        //2、过滤 开窗聚合，统计商品点击量
        Table t1 = tableEnv.sqlQuery("select " +
                "itemId, " +
                "window_start stt, " +
                "window_end edt, " +
                "count(*) ct " +
                "from table( " +
                "hop(table ub, descriptor(et), intervaL '1' hour, interval '2' hour) " +
                ")" +
                "where behavior = 'pv' " +
                "group by window_start,window_end,itemId");

        tableEnv.createTemporaryView("t1", t1);

        //3.使用over函数，对窗口进行降序排序，分配名次
        Table t2 = tableEnv.sqlQuery("select " +
                "stt,edt,itemId,ct, " +
                "row_number() over(partition by edt order by ct desc) rk " +
                "from t1");

        tableEnv.createTemporaryView("t2", t2);

        //4、过滤出排名小于三的
        Table t3 = tableEnv.sqlQuery("select " +
                "edt w_end, " +
                "itemId item_id, " +
                "ct item_count, " +
                "rk " +
                "from t2 where rk<=3");

        //写入SQL中
        //在Flink建动态表与mysql中的表关联
        tableEnv.executeSql("CREATE TABLE `hot_item` (" +
                "  `w_end` timestamp," +
                "  `item_id` bigint," +
                "  `item_count` bigint," +
                "  `rk` bigint," +
                "  PRIMARY KEY (`w_end`,`rk`)not enforced" +
                ")with(" +
                "   'connector' = 'jdbc', " +
                "   'url' = 'jdbc:mysql://hadoop162:3306/flink_sql'," +
                "   'table-name' = 'hot_item', " +
                "   'username' = 'root', " +
                "   'password' = 'aaaaaa'" +
                ")");

        //把结果写入到动态表中，则自动会写入到mysql
        t3.executeInsert("hot_item");

    }
}

