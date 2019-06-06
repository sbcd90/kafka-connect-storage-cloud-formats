package io.confluent.connect.s3.format.orc;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.storage.format.RecordWriter;
import io.confluent.connect.storage.format.RecordWriterProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OrcRecordWriterProvider implements RecordWriterProvider<S3SinkConnectorConfig> {
    private static final Logger log = LoggerFactory.getLogger(OrcRecordWriterProvider.class);
    private static final String EXTENSION = ".orc";
    private final S3Storage storage;
    private final AvroData avroData;

    OrcRecordWriterProvider(S3Storage storage, AvroData avroData) {
        this.storage = storage;
        this.avroData = avroData;
    }

    @Override
    public String getExtension() {
        return EXTENSION;
    }

    @Override
    public RecordWriter getRecordWriter(final S3SinkConnectorConfig s3SinkConnectorConfig, final String filename) {
        return new OrcRecordWriter(filename, EXTENSION, avroData, s3SinkConnectorConfig, storage);
    }
}