package io.confluent.connect.s3;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import io.confluent.connect.s3.format.orc.OrcUtils;
import io.confluent.connect.s3.util.FileUtils;
import io.confluent.connect.storage.common.StorageCommonConfig;
import io.findify.s3mock.S3Mock;
import org.apache.kafka.common.TopicPartition;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public class TestWithMockedS3 extends S3SinkConnectorTestBase {

    private static final Logger log = LoggerFactory.getLogger(TestWithMockedS3.class);

    protected S3Mock s3Mock;
    protected String port;

    @Rule
    public TemporaryFolder s3mockRoot = new TemporaryFolder();

    @Override
    protected Map<String, String> createProps() {
        Map<String, String> props = super.createProps();
        props.put(StorageCommonConfig.DIRECTORY_DELIM_CONFIG, "_");
        props.put(StorageCommonConfig.FILE_DELIM_CONFIG, "#");
        return props;
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        port = url.substring(url.lastIndexOf(":") + 1);
        File s3mockDir = s3mockRoot.newFolder("s3-tests-" + UUID.randomUUID().toString());
        System.out.println("Create folder: " + s3mockDir.getCanonicalPath());
        s3Mock = S3Mock.create(Integer.parseInt(port), s3mockDir.getCanonicalPath());
        s3Mock.start();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        s3Mock.shutdown();
    }

    public static List<S3ObjectSummary> listObjects(String bucket, String prefix, AmazonS3 s3) {
        List<S3ObjectSummary> objects = new ArrayList<>();
        ObjectListing listing;

        try {
            if (prefix == null) {
                listing = s3.listObjects(bucket);
            } else {
                listing = s3.listObjects(bucket, prefix);
            }

            objects.addAll(listing.getObjectSummaries());
            while (listing.isTruncated()) {
                listing = s3.listNextBatchOfObjects(listing);
                objects.addAll(listing.getObjectSummaries());
            }
        } catch (AmazonS3Exception e) {
            log.warn("listObjects for bucket '{}' prefix '{}' returned error code: {}", bucket, prefix, e.getStatusCode());
        }
        return objects;
    }

    public static Collection<Object> readRecords(String topicsDir, String directory, TopicPartition tp, long startOffset,
                                                 String extension, String zeroPadFormat, String bucketName, AmazonS3 s3) throws IOException {
        String fileKey = FileUtils.fileKeyToCommit(topicsDir, directory, tp, startOffset,
                extension, zeroPadFormat);
        if (extension.startsWith(".orc")) {
            return readRecordsOrc(bucketName, fileKey, s3);
        } else {
            throw new IllegalArgumentException("Unknown extension: " + extension);
        }
    }

    public static Collection<Object> readRecordsOrc(String bucketName, String fileKey, AmazonS3 s3) {
        log.debug("Reading records from bucket '{}' key '{}': ", bucketName, fileKey);
        InputStream in = s3.getObject(bucketName, fileKey).getObjectContent();
        return OrcUtils.getRecords(in, fileKey);
    }

    @Override
    public AmazonS3 newS3Client(S3SinkConnectorConfig config) {
        final AWSCredentialsProvider provider = new AWSCredentialsProvider() {
            private final AnonymousAWSCredentials credentials = new AnonymousAWSCredentials();
            @Override
            public AWSCredentials getCredentials() {
                return credentials;
            }

            @Override
            public void refresh() {

            }
        };

        AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard()
                .withAccelerateModeEnabled(config.getBoolean(S3SinkConnectorConfig.WAN_MODE_CONFIG))
                .withPathStyleAccessEnabled(true)
                .withCredentials(provider);

        builder = url == null ?
                builder.withRegion(config.getString(S3SinkConnectorConfig.REGION_CONFIG)):
                builder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(url, ""));

        return builder.build();
    }
}