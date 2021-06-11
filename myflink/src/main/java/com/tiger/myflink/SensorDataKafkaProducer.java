package com.tiger.myflink;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @Author Zenghu
 * @Date 2021/5/24 20:36
 * @Description
 * @Version: 1.0
 **/
public class SensorDataKafkaProducer {
    private final static Logger logger = LoggerFactory.getLogger(SensorDataKafkaProducer.class);

    private static String bootstrapServer = "127.0.0.1:9092";
    private static String topic = "sensor_data";
    private static Map<String, Object> params = new HashMap<>();
    private static Schema schema;
    private static Injection<GenericRecord, byte[]> genericRecordInjection = null;

    private static boolean userAvro = true;

    static {
        params.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        params.put(ProducerConfig.ACKS_CONFIG, "all");
        params.put(ProducerConfig.RETRIES_CONFIG, 30);
        params.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        if (userAvro) {
            params.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        } else {
            params.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        }
        try {
            InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("TempSensor.avsc");
            logger.info("load schema");
            /**
             URL schemaURL = Thread.currentThread().getContextClassLoader().getResource("TempSensor.avsc");
             String decode = URLDecoder.decode(schemaURL.toString(), "UTF-8");
             File file = new File(decode.substring(6));
             **/
            Schema.Parser parser = new Schema.Parser();
            schema = parser.parse(inputStream);
            genericRecordInjection = GenericAvroCodecs.toBinary(schema);
            inputStream.close();
        } catch (IOException e) {
            logger.error("init schema error", e);
        }
    }


    public static void main(String[] args) throws InterruptedException {

        KafkaProducer producer = new KafkaProducer<>(params);
        Random random = new Random();
        while (true) {
            for (int i = 1; i <= 10; i++) {
                String id = "sensor_" + i;
                long time = System.currentTimeMillis() / 1000L;
                double temp = random.nextGaussian() * 30 + 30;
                ProducerRecord record = produceTempSensorData(id, time, temp);

                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                    }
                });
            }

            TimeUnit.SECONDS.sleep(1);
        }
    }


    public static ProducerRecord produceTempSensorData(String id, long time, double temp) {
        if (userAvro) {
            GenericData.Record record = new GenericData.Record(schema);
            record.put("id", id);
            record.put("time", time);
            record.put("temp", temp);
            return new ProducerRecord<String, byte[]>(topic, genericRecordInjection.apply(record));
        } else {
            return new ProducerRecord<String, String>(topic, id, id + "," + time + "," + temp);
        }
    }


}
