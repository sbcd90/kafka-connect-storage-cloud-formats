package io.confluent.connect.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import io.confluent.connect.s3.format.orc.OrcFormat;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.s3.util.FileUtils;
import io.confluent.connect.storage.partitioner.DefaultPartitioner;
import io.confluent.connect.storage.partitioner.Partitioner;
import io.confluent.kafka.serializers.NonRecordContainer;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaProjector;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

public class DataWriterOrcTest extends TestWithMockedS3 {

    private static final Logger log = LoggerFactory.getLogger(DataWriterOrcTest.class);

    private static final String ZERO_PAD_FMT = "%010d";

    private final String extension = ".orc";

    protected S3Storage storage;
    protected AmazonS3 s3;
    protected Partitioner<FieldSchema> partitioner;

    private S3SinkTask task;
    private Map<String, String> localProps = new HashMap<>();
    protected OrcFormat format;

    @Override
    protected Map<String, String> createProps() {
        Map<String, String> props = super.createProps();
        props.putAll(localProps);
        return props;
    }

    public void setUp() throws Exception {
        super.setUp();

        s3 = PowerMockito.spy(newS3Client(connectorConfig));

        storage = new S3Storage(connectorConfig, url, S3_TEST_BUCKET_NAME, s3);

        partitioner = new DefaultPartitioner<>();
        partitioner.configure(parsedConfig);
        format = new OrcFormat(storage);

        s3.createBucket(S3_TEST_BUCKET_NAME);
        assertTrue(s3.doesBucketExist(S3_TEST_BUCKET_NAME));
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        localProps.clear();
    }

    private void testWriteRecords(String extension) throws Exception {
        setUp();
        task = new S3SinkTask(connectorConfig, context, storage, partitioner, format, SYSTEM_TIME);

        List<SinkRecord> sinkRecords = createRecords(7);
        task.put(sinkRecords);
        task.close(context.assignment());
        task.stop();

        long[] validOffsets = {0, 3, 6};
        verify(sinkRecords, validOffsets, extension);
    }

    @Test
    public void testUncompressedCompressionWriteRecords() throws Exception {
        testWriteRecords(this.extension);
    }

    protected void verify(List<SinkRecord> sinkRecords, long[] validOffsets, String extension) throws IOException {
        verify(sinkRecords, validOffsets, Collections.singleton(new TopicPartition(TOPIC, PARTITION)), false, extension);
    }

    protected void verify(List<SinkRecord> sinkRecords, long[] validOffsets, Set<TopicPartition> partitions,
                          boolean skipFileListing, String extension) throws IOException {
        if (!skipFileListing) {
            verifyFileListing(validOffsets, partitions, extension);
        }

        for (TopicPartition tp: partitions) {
            for (int i=1, j=0; i < validOffsets.length; i++) {
                long startOffset = validOffsets[i - 1];
                long size = validOffsets[i] - startOffset;

                FileUtils.fileKeyToCommit(topicsDir, getDirectory(tp.topic(), tp.partition()),
                        tp, startOffset, extension, ZERO_PAD_FMT);
                Collection<Object> records = readRecords(topicsDir, getDirectory(tp.topic(), tp.partition()), tp, startOffset,
                        extension, ZERO_PAD_FMT, S3_TEST_BUCKET_NAME, s3);
                assertEquals(size, records.size());
                verifyContents(sinkRecords, j, records);
                j += size;
            }
        }
    }

    protected void verifyFileListing(long[] validOffsets, Set<TopicPartition> partitions, String extension) {
        List<String> expectedFiles = new ArrayList<>();
        for (TopicPartition tp: partitions) {
            expectedFiles.addAll(getExpectedFiles(validOffsets, tp, extension));
        }
        verifyFileListing(expectedFiles);
    }

    protected void verifyFileListing(List<String> expectedFiles) {
        List<S3ObjectSummary> summaries = listObjects(S3_TEST_BUCKET_NAME, null, s3);
        List<String> actualFiles = new ArrayList<>();
        for (S3ObjectSummary summary: summaries) {
            String fileKey = summary.getKey();
            actualFiles.add(fileKey);
        }

        Collections.sort(actualFiles);
        Collections.sort(expectedFiles);
        assertThat(actualFiles, is(expectedFiles));
    }

    protected void verifyContents(List<SinkRecord> expectedRecords, int startIndex, Collection<Object> records) {
        Schema expectedSchema = null;
        for (Object avroRecord: records) {
            if (expectedSchema == null) {
                expectedSchema = expectedRecords.get(0).valueSchema();
            }
            Object expectedValue = SchemaProjector.project(expectedRecords.get(startIndex).valueSchema(),
                    expectedRecords.get(startIndex++).value(),
                    expectedSchema);
            Object value = format.getAvroData().fromConnectData(expectedSchema, expectedValue);

            if (value instanceof NonRecordContainer) {
                value = ((NonRecordContainer) value).getValue();
            }
            if (avroRecord instanceof Utf8) {
                assertEquals(value, avroRecord.toString());
            } else {
                assertEquals(value, avroRecord);
            }
        }
    }

    protected String getDirectory(String topic, int partition) {
        String encodedPartition = "partition=" + partition;
        return partitioner.generatePartitionedPath(topic, encodedPartition);
    }

    protected List<String> getExpectedFiles(long[] validOffsets, TopicPartition tp, String extension) {
        List<String> expectedFiles = new ArrayList<>();
        for (int i=1; i <= validOffsets.length; i++) {
            long startOffset = validOffsets[i - 1];
            expectedFiles.add(FileUtils.fileKeyToCommit(topicsDir, getDirectory(tp.topic(), tp.partition()), tp,
                    startOffset, extension, ZERO_PAD_FMT));
        }
        return expectedFiles;
    }

    protected List<SinkRecord> createRecords(int size) {
        return createRecords(size, 0);
    }

    protected List<SinkRecord> createRecords(int size, long startOffset) {
        return createRecords(size, startOffset, Collections.singleton(new TopicPartition(TOPIC, PARTITION)));
    }

    protected List<SinkRecord> createRecords(int size, long startOffset, Set<TopicPartition> partitions) {
        String key = "key";
        Schema schema = createSchema();
        Struct record = createRecord(schema);

        List<SinkRecord> sinkRecords = new ArrayList<>();
        for (long offset = startOffset; offset < startOffset + size; offset++) {
            for(TopicPartition tp: partitions) {
                sinkRecords.add(new SinkRecord(TOPIC, tp.partition(), Schema.STRING_SCHEMA, key, schema, record, offset));
            }
        }
        return sinkRecords;
    }
}