package cn.gk.test;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @Author: HT
 * @Date: 2022/6/13
 * @Description: cn.gk.test.Test
 */
public class Test {
    public static void main(String[] args) throws Exception {

        //   设置用户为 hadoop 访问 hdfs
        System.setProperty("HADOOP_USER_NAME", "myhdp");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //   设置任务停掉时,保存checkpoint的目录
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //   设置状态后端, 设置为 true 可以增量的做ck
        EmbeddedRocksDBStateBackend rocksDBStateBackend = new EmbeddedRocksDBStateBackend(true);
        //   设置状态后端为 rocksDB
        env.setStateBackend(rocksDBStateBackend);
        //   15 分钟 做一次ck , 选择 ck的模式
        //   使用状态 做ck才会容错, 如果在ck之前出错,则不会容错
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hdp01:8020/checkpoint-dir");

        //   设置重启策略  [故障率重启] 重启3次,60s内,每个5s重启一次
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.seconds(60), Time.seconds(5)));
        //   两个 checkpoint 之间的时间间隔 1ms  设置为 100ms
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(100);
        //   同一时间只允许 1个 checkpoint 进行 [ck的并发数] maxConcurrentCheckpoints
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        //   开启后检查点不会等待对其,提高性能[减少背压], 但是只有在精确一次的检查点并且允许的最大检查点并发数量为1的情况下才能使用
        env.getCheckpointConfig().enableUnalignedCheckpoints();
        //   Checkpoint 必须在60分钟内完成，否则就会被抛弃 [默认是10分钟
        env.getCheckpointConfig().setCheckpointTimeout(100000);
        //   允许 两个 连续的 checkpoint 错误
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(2);

        DataStreamSource<String> stringDataStreamSource = env.addSource(new ClickSource());

        DataStream<Tuple2<String, Integer>> wordCountDataStream = stringDataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {

                        String[] words = value.split(",");
                        for (String word : words) {
                            if ("error".equals(word)) {
                                throw new Exception();
                            }
                            out.collect(Tuple2.of(word, 1));
                        }
                    }
                }).keyBy(tp -> tp.f0)
                .map(new RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
                    //定义一个变量保存中间结果或者是初始值
                    private transient ValueState<Integer> stateCount;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        /**
                         *  创建一个状态描述器
                         *  参数一:状态的名称
                         *  参数二:保存状态的类型
                         */
                        ValueStateDescriptor<Integer> stateDescriptor = new ValueStateDescriptor<>("wc-state", Integer.class);
                        //获取状态
                        stateCount = getRuntimeContext().getState(stateDescriptor);
                    }

                    @Override
                    public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
                        //获取当前输入的
                        Integer correctCnt = value.f1;
                        //获取历史结果
                        Integer historyCnt = stateCount.value();

                        //判断如果历史结果是否为 null 如果是null(程序第一次启动时,没有历史数据)那么就赋初始值
                        if (historyCnt == null) {
                            historyCnt = 0;
                        }
                        Integer total = historyCnt + correctCnt;
                        //将中间值更新到stateCount
                        stateCount.update(total);
                        value.f1 = total;
                        return value;
                    }
                });


        wordCountDataStream.addSink(
                JdbcSink.sink(
                        " replace into wc(word,count) values(?,?) ",
                        new JdbcStatementBuilder<Tuple2<String, Integer>>() {
                            @Override
                            public void accept(PreparedStatement ps, Tuple2<String, Integer> tp2) throws SQLException {

                                String word = tp2.f0;
                                Integer count = tp2.f1;
                                // TODO 设置值
                                ps.setString(1, word);
                                ps.setInt(2, count);
                            }
                        },
                        JdbcExecutionOptions.builder()
                                .withBatchIntervalMs(1000) // 数据多久刷新一次 5s
                                .withBatchSize(10) //数据达到多少条数据时会刷新
                                .withMaxRetries(3) //重试的次数
                                .build(),
                        new JdbcConnectionOptions
                                .JdbcConnectionOptionsBuilder()
                                .withUrl("jdbc:mysql://192.168.10.101:3306/test?characterEncoding=utf-8&useSSL=false&rewriteBatchedStatements=true")
                                .withDriverName("com.mysql.jdbc.Driver")
                                .withUsername("root")
                                .withPassword("000000")
                                .build()
                )
        );

        env.execute();

    }
}
