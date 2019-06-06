package io.confluent.connect.s3.util;

import org.apache.commons.lang.StringUtils;
import org.apache.kafka.common.TopicPartition;

public class FileUtils {
    public static final String TEST_FILE_DELIM = "#";
    public static final String TEST_DIRECTORY_DELIM = "_";

    public static String fileKey(String topicsPrefix, String keyPrefix, String name) {
        String suffix = keyPrefix + TEST_FILE_DELIM + name;
        return StringUtils.isNotBlank(suffix)
                ? topicsPrefix + TEST_DIRECTORY_DELIM + suffix
                : suffix;
    }

    public static String fileKeyToCommit(String topicsPrefix, String dirPrefix, TopicPartition tp,
                                         long startOffset, String extension, String zeroPadFormat) {
        String name = tp.topic()
                + TEST_FILE_DELIM
                + tp.partition()
                + TEST_FILE_DELIM
                + String.format(zeroPadFormat, startOffset)
                + extension;
        return fileKey(topicsPrefix, dirPrefix, name);
    }
}