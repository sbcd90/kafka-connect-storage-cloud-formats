package io.confluent.connect.s3.format.orc;

import io.confluent.connect.avro.AvroData;
import org.apache.kafka.connect.sink.SinkRecord;

import java.io.InputStream;
import java.util.Collection;

public class OrcUtils {

    public static Collection<Object> getRecords(InputStream inputStream, String fileKey) {
        return null;
    }

    public static byte[] putRecords(Collection<SinkRecord> records, AvroData avroData) {
        return null;
    }
}