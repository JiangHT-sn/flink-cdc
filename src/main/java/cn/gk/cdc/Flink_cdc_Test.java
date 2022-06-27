package cn.gk.cdc;

import cn.gk.pojo.DatabaseInfo;
import com.alibaba.fastjson.JSON;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Properties;

/**
 * @Author：HT
 * @Date：2022/6/22
 * @Description： flink-cdc 版本支持[ flink1.13 ] flink-sql 1.12 不支持 2.0版本
 * <p>
 * 特点
 * 支持读取数据库快照，即使发生故障，只要处理一次就可以继续读取Binlog
 * CDC connectors for DataStream API，用户可以在一个作业中使用多个数据库和表上的更改，而无需部署Debezium和Kafka。
 * CDC connectors for Table/SQL API，用户可以使用SQL DDL创建CDC源来监视单个表上的更改。
 * <p>
 * 通过 高位点 和 低位点 对数据做判断
 * 初始位置做地位点标记
 * select * from tbName 拿到初始化的所有数据的时候
 * 记录高位点 [先把数据缓存到buffer中 !!!]
 * 从地位点到高位点 即:读取初始化数据的这段时间,再来数据 []
 * 根据主键到buffer中更新数据,然后输出
 * 随后就是增量读取实时变化的数据
 */
public class Flink_cdc_Test {
    public static void main(String[] args) throws Exception {

        // kafka的配置参数
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hdp01:9092,hdp02:9092,hdp03:9092");

        // 获取执行环境对象
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 获取 mysqlSource
        // streamAPI 可以监控 多库多表
        DebeziumSourceFunction<String> mysqlSourceStream = MySqlSource.<String>builder()
                .hostname(" ")
                .port(3306)
                .username(" ")
                .password(" ")
                .databaseList("flink_test")  // 可变参数
                .tableList("flink_test.tb_user,flink_test.tb_use,flink_test.tb_category")  // 可变参数
                .deserializer(new CustomStringDeserializationSchema())  // 指定序列化器[可以自定义] CustomStringDeserializationSchema
                //.deserializer(new StringDebeziumDeserializationSchema()) 提供的序列化器
                .startupOptions(StartupOptions.initial())  // 指定监听的模式[程序初始化开始监听]
                .build();

        // 添加 source
        DataStreamSource<String> mysqlDataStreamSource = env.addSource(mysqlSourceStream);

        // {"op":"read","database":"flink_test","before":{},"after":{"name":"ddd","id":4},"table":"tb_user"}
        // insert 时 before 数据为 null after不为 null
        // delete 时 before 数据就不为 null after为 null

        // TODO 定义一个测输出流

        OutputTag<String> useOutputTag = new OutputTag<String>("tb_use-output") {
        };

        OutputTag<String> categoryOutputTag = new OutputTag<String>("tb_category-output") {
        };

        /**
         * 监控多个表时可以使用分流操作
         */
        SingleOutputStreamOperator<String> jsonMainBeanDataStream = mysqlDataStreamSource.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String s, Context context, Collector<String> out) throws Exception {
                // TODO 获取 表明
                DatabaseInfo databaseInfo = JSON.parseObject(s, DatabaseInfo.class);

                if ("flink_test".equals(databaseInfo.getDatabase()) && "tb_user".equals(databaseInfo.getTable())) {
                    // TODO 输出 tb_user 表
                    out.collect(s);
                } else if ("flink_test".equals(databaseInfo.getDatabase()) && "tb_use".equals(databaseInfo.getTable())) {
                    // TODO 输出 tb_use  表
                    context.output(useOutputTag, s);
                } else if ("flink_test".equals(databaseInfo.getDatabase()) && "tb_category".equals(databaseInfo.getTable())) {
                    // TODO 输出 tb_use  表
                    context.output(categoryOutputTag, s);
                }
            }
        });

        DataStream<String> useDataStream = jsonMainBeanDataStream.getSideOutput(useOutputTag);
        DataStream<String> categoryOutputTagDataStream = jsonMainBeanDataStream.getSideOutput(categoryOutputTag);
        jsonMainBeanDataStream.print("tb_user.....");
        useDataStream.print("tb_use......");
        categoryOutputTagDataStream.print("category........");

        /**
         * 根据表的不同去解析不同的数据
         */


        // 写出数据
        // mysqlDataStreamSource.addSink(KafkaProducerSink.getStringFlinkKafkaProducer("", properties));



                /* System.out.println("==========================================================================");
                 // 可以使用flink-sql来完成[但是一次只能监控一个库下的一个表]
                 // 获取表执行环境对象
                 EnvironmentSettings settings = EnvironmentSettings.newInstance()
                         .inStreamingMode()
                         .useBlinkPlanner()
                         .build();
                 StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

                 // flinkSql 不需要配置 反序列化器
                 tableEnv.executeSql(
                         "CREATE TABLE mysql_binlog (  " +
                                 " word STRING NOT NULL,       " +
                                 " count  INT,           " +
                                 " PRIMARY KEY(word) NOT ENFORCED " +
                                 ") WITH ( " +
                                 " 'connector' = 'mysql-cdc', " +
                                 " 'scan.startup.mode' = 'initial', " +
                                 " 'hostname' = 'udp01', " +
                                 " 'port' = '3306', " +
                                 " 'username' = 'root', " +
                                 " 'password' = '000000',   " +
                                 " 'database-name' = 'test', " +
                                 " 'table-name' = 'wc' " +
                                 ")"
                 );
                 Table sqlQuery = tableEnv.sqlQuery("select * from mysql_binlog");
                 tableEnv.toRetractStream(sqlQuery, Row.class).print();*/


        env.execute();

    }
}
