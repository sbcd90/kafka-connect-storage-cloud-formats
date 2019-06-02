package io.confluent.connect.s3.format;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.storage.format.RecordWriter;
import io.confluent.connect.storage.format.RecordWriterProvider;
import org.apache.avro.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.orc.Writer;
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
        return new RecordWriter() {
            Schema schema = null;
            boolean committed = false;
            Writer orcWriter;

            @Override
            public void write(SinkRecord sinkRecord) {

            }

            @Override
            public void close() {

            }

            @Override
            public void commit() {

            }
        };
    }
}