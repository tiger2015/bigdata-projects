package com.tiger.myflink.table;

import com.tiger.myflink.TempSensorData;
import com.tiger.myflink.common.FlinkSourceStreamUtil;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.types.logical.DateType;

/**
 * @Author Zenghu
 * @Date 2021/6/9 20:41
 * @Description
 * @Version: 1.0
 **/
public class FlinkTableConnectorToFile {


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


       // Table result = tableEnv.sqlQuery("select id as sensor_id, max(temp) as max_temp, min(temp) as min_temp, avg(temp) as avg_temp from sensor group by id");


        Table result = tableEnv.sqlQuery("select id, `time`, temp from sensor where id='sensor_1'");


        result.printSchema();

        // 不支持update和delete操作
        tableEnv.connect(new FileSystem().path("data/sensor_result.txt"))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("time", DataTypes.BIGINT())
                        .field("temp", DataTypes.DOUBLE()))
                .createTemporaryTable("sensor_temp");

        result.insertInto("sensor_temp");


        env.execute();
    }


}
