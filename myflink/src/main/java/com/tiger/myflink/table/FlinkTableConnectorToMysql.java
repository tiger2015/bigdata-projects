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
 * @Date 2021/6/9 20:41
 * @Description
 * @Version: 1.0
 **/
public class FlinkTableConnectorToMysql {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<GenericRecord> sensor_data = env.addSource(FlinkSourceStreamUtil.createKafkaSourceStreamUseAvro("sensor_data"));

        SingleOutputStreamOperator<TempSensorData> sensorDataStream = sensor_data.map(value -> {
            return new TempSensorData(value.get("id").toString(), (long) value.get("time"), (double) value.get("temp"));
        });

        // 使用新的
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, bsSettings);

        tableEnv.createTemporaryView("sensor", sensorDataStream);


        Table result = tableEnv.sqlQuery("select id as sensor_id, max(temp) as max_temp, min(temp) as min_temp, avg(temp) as avg_temp from sensor group by id");


        String ddl = "CREATE TABLE SensorTempTable(" +
                "sensor_id STRING," +
                "max_temp DOUBLE," +
                "min_temp DOUBLE," +
                "avg_temp DOUBLE" +
                ") WITH(" +
                "'connector.type'='jdbc'," +
                "'connector.url' = 'jdbc:mysql://127.0.0.1:3306/test'," +
                "'connector.table' = 'sensor_temp'," +
                "'connector.driver' = 'com.mysql.jdbc.Driver'," +
                "'connector.username' = 'test'," +
                "'connector.password' = 'test'," +
                "'connector.write.flush.interval' = '2s'"+
                ")";

        tableEnv.sqlUpdate(ddl);

        result.insertInto("SensorTempTable");



        env.execute();
    }


}
