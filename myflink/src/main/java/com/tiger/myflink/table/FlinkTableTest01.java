package com.tiger.myflink.table;

import com.tiger.myflink.TempSensorData;
import com.tiger.myflink.common.FlinkSourceStreamUtil;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @Author Zenghu
 * @Date 2021/6/8 21:26
 * @Description
 * @Version: 1.0
 **/
public class FlinkTableTest01 {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<GenericRecord> sensor_data = env.addSource(FlinkSourceStreamUtil.createKafkaSourceStreamUseAvro("sensor_data"));

        SingleOutputStreamOperator<TempSensorData> sensorDataStream = sensor_data.map(value -> {
            return new TempSensorData(value.get("id").toString(), (long) value.get("time"), (double) value.get("temp"));
        });

        // 使用旧的，默认使用旧的
       // EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();

        // 使用新的
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, bsSettings);

        tableEnv.createTemporaryView("sensor", sensorDataStream);


        Table result = tableEnv.sqlQuery("select id, max(temp), min(temp), avg(temp) from sensor group by id");
        DataStream<Tuple2<Boolean, Row>> resultStream = tableEnv.toRetractStream(result, Row.class);

        resultStream.print();


        env.execute();
    }


}
