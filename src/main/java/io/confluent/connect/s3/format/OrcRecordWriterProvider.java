package io.confluent.connect.s3.format;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.storage.format.RecordWriter;
import io.confluent.connect.storage.format.RecordWriterProvider;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

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
                if (schema == null) {
                    schema = sinkRecord.valueSchema();
                    try {
                        log.info("Opening record writer for: {}", filename);
                        org.apache.avro.Schema avroSchema = avroData.fromConnectSchema(schema);

                        TypeDescription orcSchema = this.createOrcSchema(avroSchema);

                        Configuration orcConfig = this.getOrcConfig();
                        FileSystem s3FileSystem = this.getS3FileSystem(orcConfig);

                        orcWriter = OrcFile.createWriter(new Path(filename + "." + EXTENSION),
                                OrcFile.writerOptions(orcConfig).setSchema(orcSchema)
                                    .fileSystem(s3FileSystem));

                        VectorizedRowBatch rowBatch = orcSchema.createRowBatch();
                        List<ColumnVector> columnVectors = this.getColumnVectors(rowBatch, orcSchema,
                                orcSchema.getChildren().size());

                        Object value = avroData.fromConnectData(schema, sinkRecord.value());
                        GenericRecord currRecord = (GenericRecord) value;


                    } catch (IOException e) {
                        throw new ConnectException(e);
                    }
                }
            }

            private TypeDescription createOrcSchema(org.apache.avro.Schema avroSchema) {
                List<org.apache.avro.Schema.Field> fields = avroSchema.getFields();

                StringBuilder orcFields = new StringBuilder("struct<");
                for (int count=0; count < fields.size(); count++) {
                    String fieldName = fields.get(count).name();
                    org.apache.avro.Schema fieldSchema = fields.get(count).schema();
                    List<org.apache.avro.Schema> typesOf = fieldSchema.getTypes();
                    String typeOf = typesOf.get(0).getType().getName();

                    orcFields.append(fieldName + ":" + typeOf);

                    if (count < (fields.size() - 1)) {
                        orcFields.append(",");
                    }
                }
                orcFields.append(">");
                return TypeDescription.fromString(orcFields.toString());
            }

            private Configuration getOrcConfig() {
                String awsAccessKeyId = System.getenv("AWS_ACCESS_KEY_ID");
                String awsSecretKeyId = System.getenv("AWS_SECRET_ACCESS_KEY");

                Configuration orcConfig = new Configuration();
                orcConfig.set("fs.s3a.access.key", awsAccessKeyId);
                orcConfig.set("fs.s3a.secret.key", awsSecretKeyId);
                orcConfig.set("fs.s3a.endpoint", storage.url());
                orcConfig.set("fs.s3a.path.style.access", "true");

                return orcConfig;
            }

            private FileSystem getS3FileSystem(Configuration orcConfig) throws IOException {
                URI s3Uri = URI.create("s3://" + s3SinkConnectorConfig.getBucketName());

                FileSystem s3FileSystem = new S3AFileSystem();
                s3FileSystem.initialize(s3Uri, orcConfig);
                s3FileSystem.setWorkingDirectory(new Path("/"));

                return s3FileSystem;
            }

            private List<ColumnVector> getColumnVectors(VectorizedRowBatch rowBatch,
                                                        TypeDescription orcSchema,
                                                        int noOfFields) {
                List<ColumnVector> columnVectors = new ArrayList<>();

                for (int count = 0; count < noOfFields; count++) {
                    String typeOf = orcSchema.getChildren().get(count).getCategory().getName();
                    switch (typeOf) {
                        case "int":
                            LongColumnVector longColumnVector = (LongColumnVector) rowBatch.cols[count];
                            columnVectors.add(longColumnVector);
                            break;
                        case "string":
                            BytesColumnVector bytesColumnVector = (BytesColumnVector) rowBatch.cols[count];
                            columnVectors.add(bytesColumnVector);
                            break;
                        default:
                            throw new UnsupportedOperationException("type is not supported");
                    }
                }
                return columnVectors;
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