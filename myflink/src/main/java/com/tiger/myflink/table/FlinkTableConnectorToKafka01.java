package com.tiger.myflink.table;

import com.tiger.myflink.TempSensorData;
import com.tiger.myflink.common.FlinkSourceStreamUtil;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Avro;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.producer.ProducerConfig;

/**
 * @Author Zenghu
 * @Date 2021/6/8 21:26
 * @Description
 * @Version: 1.0
 **/
public class FlinkTableConnectorToKafka01 {


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


        Table result = tableEnv.sqlQuery("select id, `time`, temp from sensor where id='sensor_1'");



        String ddl = "" +
                "CREATE TABLE KafkaTable (\n" +
                "  `id` STRING,\n" +
                "  `time` BIGINT,\n" +
                "  `temp` DOUBLE\n" +
                ") WITH (\n" +
                "  'connector.type' = 'kafka',\n" +
                "  'connector.version' = 'universal'," +
                "  'connector.topic' = 'sensor_result',\n" +
                "  'connector.properties.bootstrap.servers' = '127.0.0.1:9092',\n" +
                "  'connector.properties.group.id' = 'testGroup',\n" +
                "  'connector.startup-mode' = 'earliest-offset',\n" +
                "  'format.type' = 'csv'\n" +
                ")";


        tableEnv.connect(new Kafka()
                .version("universal")
                .topic("sensor_result")
                .property(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092"))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("time", DataTypes.BIGINT())
                        .field("temp", DataTypes.DOUBLE()))
                .inAppendMode()
                .createTemporaryTable("sensor_temp");

        //tableEnv.sqlUpdate(ddl);

        tableEnv.insertInto("sensor_temp", result);


        env.execute();
    }


}
