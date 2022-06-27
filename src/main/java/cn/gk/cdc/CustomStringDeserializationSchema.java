package cn.gk.cdc;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

/**
 * @Author：HT
 * @Date：2022/6/22
 * @Description： 自定义实现反序列化器
 */
/*
SourceRecord 数据格式
 TODO op 会有 c r u d 操作
SourceRecord{sourcePartition=server=mysql_binlog_source},
sourceOffset={transaction_id=null, ts_sec=1629277152，file=mysq1-bin.000199, pos=4490,row=1, server_id=1, event=2}}
ConnectRecord{topic='mysql_binlog_source.test.wc', kafkaPartition=null,
key=Struct{id=1005},keySchema=Schema{mysql_binlog_source.test.wc.Key:STRUCT},
Value=Struct{
before=Struct{id=1005,name=aaa,sex=male},
after=Struct{id=1005,name=ddd,sex=male},
source=Struct{version=1.5.2.Final,connector=mysql,name=mysq1_binlog_source,ts_ms=1629277152000,db=cdc_test,table=user_info,server_id=1,file=mysql-bin.000199,pos=4630,row=0},
op=u,
ts_ms=1629277152505
},
valueSchema=Schema{mysql_binlog_source.cdc_test.user_info.Envelope:STRUCT},
timestamp=null, headers=ConnectHeaders(headers=)}
 */

/**
 * pos 位置信息
 * 先找出我们要的数据
 * topic {db , table}
 */
public class CustomStringDeserializationSchema implements DebeziumDeserializationSchema<String> {
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> out) throws Exception {

        // TODO 创建json对象用户封装结果
        JSONObject outPutJsonObject = new JSONObject();
        // 一行数据的封装类 SourceRecord
        String topic = sourceRecord.topic();
        String[] arr = topic.split("\\.");
        String dataBaseName = arr[1];
        String tableName = arr[2];
        outPutJsonObject.put("database", dataBaseName);
        outPutJsonObject.put("table", tableName);

        // TODO 获取 before(操作前的数据) 和 after(操作后的数据)

        // 注意 Struct 是 kafka.connect.data下的
        Struct beforeValue = (Struct) sourceRecord.value();
        Struct beforeStruct = beforeValue.getStruct("before");
        StructFieldToJsonObject(beforeStruct, outPutJsonObject, "before");

        // 获取 after 数据
        Struct afterValue = (Struct) sourceRecord.value();
        Struct afterStruct = afterValue.getStruct("after");
        StructFieldToJsonObject(afterStruct, outPutJsonObject, "after");

        // 获取source的数据 source
        /*Struct sourceValue = (Struct) sourceRecord.value();
        Struct sourceStruct = sourceValue.getStruct("source");
        StructFieldToJsonObject(sourceStruct, outPutJsonObject, "source");*/

        // 获取 op数据[c r u d 操作] 做判断
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        // todo create 就是 insert
        String op = operation.toString().toLowerCase();

        if ("create".equals(op)) op = "insert";
        outPutJsonObject.put("op", op);

        // 输出
        out.collect(outPutJsonObject.toJSONString());

    }

    /**
     * @return 取输出类型
     */
    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }

    /**
     * 解析 after before 的struct工具类
     *
     * @param struct     结构体(after /  before)
     * @param jsonObject 结果数据封装的json
     * @param name       结构体的名称
     */
    private void StructFieldToJsonObject(Struct struct, JSONObject jsonObject, String name) {
        // insert 时 before 数据就是 null after不为 null
        // delete 时 before 数据就不是 null after为 null
        JSONObject object = new JSONObject();
        if (struct != null) {
            Schema schema = struct.schema();
            List<Field> fieldList = schema.fields();
            // 遍历 获取 数据
            for (Field field : fieldList) {
                object.put(field.name(), struct.get(field));
            }
        }
        jsonObject.put(name, object);
    }


}
