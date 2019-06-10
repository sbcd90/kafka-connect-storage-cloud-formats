package io.confluent.connect.s3.format.orc;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.format.RecordWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.hadoop.hive.ql.io.orc.NiFiOrcUtils;
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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class OrcRecordWriter implements RecordWriter {

    private static final Logger log = LoggerFactory.getLogger(OrcRecordWriter.class);
    private final String filename;
    private final String EXTENSION;
    private final AvroData avroData;
    private final S3SinkConnectorConfig connectorConfig;
    private final S3Storage storage;
    private VectorizedRowBatch rowBatch;
    private List<ColumnVector> columnVectors;
    private TypeDescription orcSchema;

    Schema schema = null;
    boolean committed = false;
    Writer orcWriter;

    public OrcRecordWriter(String filename, String EXTENSION,
                           AvroData avroData, S3SinkConnectorConfig connectorConfig,
                           S3Storage storage) {
        this.filename = filename.replace("#", "_");
        this.EXTENSION = EXTENSION;
        this.avroData = avroData;
        this.connectorConfig = connectorConfig;
        this.storage = storage;
    }

    @Override
    public void write(SinkRecord sinkRecord) {
        if (schema == null) {
            schema = sinkRecord.valueSchema();
            try {
                log.info("Opening record writer for: {}", filename);
                org.apache.avro.Schema avroSchema = avroData.fromConnectSchema(schema);
                this.initColumnVectors(avroSchema);

                Object value = avroData.fromConnectData(schema, sinkRecord.value());
                GenericRecord currRecord = (GenericRecord) value;

                int row = rowBatch.size++;
                this.fillColumnVectors(currRecord, avroSchema.getFields(), orcSchema, row);

                if (rowBatch.size == rowBatch.getMaxSize()) {
                    orcWriter.addRowBatch(rowBatch);
                    rowBatch.reset();
                }
            } catch (IOException e) {
                throw new ConnectException(e);
            }
        }
    }

    private void initColumnVectors(org.apache.avro.Schema avroSchema) throws IOException {
        if (this.orcWriter == null) {
            orcSchema = this.createOrcSchema(avroSchema);

            Configuration orcConfig = this.getOrcConfig();
            try {
                FileSystem s3FileSystem = this.getS3FileSystem(orcConfig, this.connectorConfig.getBucketName());

                this.orcWriter = OrcFile.createWriter(new Path(filename),
                        OrcFile.writerOptions(orcConfig).setSchema(orcSchema)
                                .fileSystem(s3FileSystem));
            } catch (Exception ex) {
                this.orcWriter = OrcFile.createWriter(new Path(filename),
                        OrcFile.writerOptions(new Configuration()).setSchema(orcSchema)
                                .fileSystem(this.getLocalFileSystem(this.connectorConfig.originals()
                                        .get("mock.dir.path").toString())));
            }

            this.rowBatch = orcSchema.createRowBatch();
            this.columnVectors = this.getColumnVectors(rowBatch, orcSchema,
                    orcSchema.getChildren().size());
        }
    }

    private TypeDescription createOrcSchema(org.apache.avro.Schema avroSchema) {
        List<org.apache.avro.Schema.Field> fields = avroSchema.getFields();

        StringBuilder orcFields = new StringBuilder("struct<");
        for (int count=0; count < fields.size(); count++) {
            String fieldName = fields.get(count).name();
            org.apache.avro.Schema fieldSchema = fields.get(count).schema();
//            List<org.apache.avro.Schema> typesOf = fieldSchema.getTypes();
            String typeOf = fieldSchema.getType().getName();

            if (typeOf.equals("long")) {
                orcFields.append(fieldName + ":" + "bigint");
            } else {
                orcFields.append(fieldName + ":" + typeOf);
            }

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
        if (awsAccessKeyId != null) {
            orcConfig.set("fs.s3a.access.key", awsAccessKeyId);
        } else {
            orcConfig.set("fs.s3a.access.key", "");
        }
        if (awsSecretKeyId != null) {
            orcConfig.set("fs.s3a.secret.key", awsSecretKeyId);
        } else {
            orcConfig.set("fs.s3a.secret.key", "");
        }
        orcConfig.set("fs.s3a.endpoint", storage.url());
        orcConfig.set("fs.s3a.path.style.access", "true");

        return orcConfig;
    }

    private FileSystem getS3FileSystem(Configuration orcConfig, String bucketName) throws IOException {
        URI s3Uri = URI.create("s3://" + bucketName);

        FileSystem s3FileSystem = new S3AFileSystem();
        s3FileSystem.initialize(s3Uri, orcConfig);
        s3FileSystem.setWorkingDirectory(new Path("/"));

        return s3FileSystem;
    }

    private FileSystem getLocalFileSystem(String location) throws IOException {
        URI localUri = URI.create(location);

        FileSystem localFileSystem = new RawLocalFileSystem();
        localFileSystem.initialize(localUri, new Configuration());
        localFileSystem.setWorkingDirectory(new Path(location + "/" +
                this.connectorConfig.get("s3.bucket.name")));
        return localFileSystem;
    }

    private List<ColumnVector> getColumnVectors(VectorizedRowBatch rowBatch,
                                                TypeDescription orcSchema,
                                                int noOfFields) {
        List<ColumnVector> columnVectors = new ArrayList<>();

        for (int count = 0; count < noOfFields; count++) {
            String typeOf = orcSchema.getChildren().get(count).getCategory().getName();
            switch (typeOf) {
                case "boolean":
                case "bigint":
                case "int":
                    LongColumnVector longColumnVector = (LongColumnVector) rowBatch.cols[count];
                    columnVectors.add(longColumnVector);
                    break;
                case "double":
                case "float":
                    DoubleColumnVector doubleColumnVector = (DoubleColumnVector) rowBatch.cols[count];
                    columnVectors.add(doubleColumnVector);
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

    private void fillColumnVectors(GenericRecord currRecord,
                                   List<org.apache.avro.Schema.Field> fields,
                                   TypeDescription orcSchema, int row) {
        for (int count=0; count < fields.size(); count++) {
            org.apache.avro.Schema.Field field = fields.get(count);
            org.apache.avro.Schema fieldSchema = field.schema();

            Object o = currRecord.get(field.name());
            Object fieldValue = NiFiOrcUtils.convertToORCObject(NiFiOrcUtils.getOrcField(fieldSchema), o);

            String typeOf = orcSchema.getChildren().get(count).getCategory().getName();
            switch (typeOf) {
                case "boolean":
                    fieldValue = Boolean.valueOf(fieldValue.toString()) ? 1: 0;
                case "bigint":
                case "int":
                    LongColumnVector longColumnVector = (LongColumnVector) columnVectors.get(count);
                    longColumnVector.vector[row] = Long.valueOf(fieldValue.toString());
                    break;
                case "double":
                case "float":
                    DoubleColumnVector doubleColumnVector = (DoubleColumnVector) columnVectors.get(count);
                    doubleColumnVector.vector[row] = Double.valueOf(fieldValue.toString());
                    break;
                case "string":
                    BytesColumnVector bytesColumnVector = (BytesColumnVector) columnVectors.get(count);
                    byte[] value = fieldValue.toString().getBytes(StandardCharsets.UTF_8);
                    bytesColumnVector.setVal(row, value, 0, value.length);
                    break;
                default:
                    throw new UnsupportedOperationException("type is not supported");
            }
        }
    }

    @Override
    public void close() {
        try {
            if (committed) {
                return;
            }
            if (rowBatch.size != 0) {
                orcWriter.addRowBatch(rowBatch);
                rowBatch.reset();
            }

            commit();
        } catch (IOException ex) {
            throw new ConnectException(ex);
        }
    }

    @Override
    public void commit() {
        try {
            committed = true;
            if (orcWriter != null) {
                orcWriter.close();
                orcWriter = null;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}