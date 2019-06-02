package io.confluent.connect.s3.format;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.storage.format.Format;
import io.confluent.connect.storage.format.RecordWriterProvider;
import io.confluent.connect.storage.format.SchemaFileReader;
import io.confluent.connect.storage.hive.HiveFactory;

public class OrcFormat implements Format<S3SinkConnectorConfig, String> {
    private final S3Storage storage;
    private final AvroData avroData;

    public OrcFormat(S3Storage storage) {
        this.storage = storage;
        this.avroData = new AvroData(storage.conf().avroDataConfig());
    }

    @Override
    public RecordWriterProvider<S3SinkConnectorConfig> getRecordWriterProvider() {
        return null;
    }

    @Override
    public SchemaFileReader<S3SinkConnectorConfig, String> getSchemaFileReader() {
        return null;
    }

    @Override
    public HiveFactory getHiveFactory() {
        return null;
    }

    public AvroData getAvroData() {
        return avroData;
    }
}