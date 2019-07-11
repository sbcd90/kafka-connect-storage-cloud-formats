package io.confluent.connect.s3.format.orc;

import io.confluent.connect.avro.AvroData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.hadoop.hive.ql.io.orc.NiFiOrcUtils;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class OrcUtils {

    public static Collection<Object> getRecords(InputStream inputStream, String fileKey) {
        try {
            File tempFile = File.createTempFile("test", ".orc");
            OutputStream os = new FileOutputStream(tempFile);
            os.write(IOUtils.toByteArray(inputStream));

            Reader reader = OrcFile.createReader(new Path(tempFile.getPath()),
                    OrcFile.readerOptions(new Configuration()));

            RecordReader rows = reader.rows();
            VectorizedRowBatch rowBatch = reader.getSchema().createRowBatch();

            List<String> fieldNames = reader.getSchema().getFieldNames();
            Schema schema = convertOrcToKafkaSchema(fieldNames,
                    reader.getSchema().getChildren());
            List<Field> fields = schema.fields();

            Collection<Object> records = new ArrayList<>();

            int count = 0;
            while (rows.nextBatch(rowBatch)) {

                for (int idx = 0; idx < rowBatch.size; idx++, count++) {
                    Struct record = new Struct(schema);
//                    System.out.println("Row- " + (count + idx));
                    for (int colIdx = 0; colIdx < rowBatch.numCols; colIdx++) {
                        ColumnVector columnVector = rowBatch.cols[colIdx];
                        String typeName = columnVector.type.name();

                        switch (typeName) {
                            case "LONG":
                                //schemaBuilder.field()
                                long[] intValues = ((LongColumnVector) columnVector).vector;
                                record.put(fieldNames.get(colIdx),
                                        convertToTargetObject(intValues[0], fields.get(colIdx)));
/*                                for (int valueIdx = 0; valueIdx < intValues.length; valueIdx++) {
                                    System.out.print(intValues[valueIdx] + " ");
                                }*/
                                break;
                            case "DOUBLE":
                                double[] doubleValues = ((DoubleColumnVector) columnVector).vector;
                                record.put(fieldNames.get(colIdx),
                                        convertToTargetObject(doubleValues[0], fields.get(colIdx)));
                                break;
                            case "BYTES":
                                int start = ((BytesColumnVector) columnVector).start[idx];
                                int length = ((BytesColumnVector) columnVector).length[idx];
                                byte[][] byteValues = ((BytesColumnVector) columnVector).vector;
                                record.put(fieldNames.get(colIdx),
                                        convertToTargetObject(new String(byteValues[0], start, length),
                                                fields.get(colIdx)));
//                                System.out.print(new String(byteValues[0], start, length) + ", ");
                                break;
                        }
                    }
                    records.add(record);
                }
            }
            rows.close();
            tempFile.delete();
            return records;
        } catch (IOException ex) {
            return Arrays.asList();
        }
    }

    private static Schema convertOrcToKafkaSchema(List<String> fieldNames, List<TypeDescription> fields) {
        SchemaBuilder schemaBuilder = SchemaBuilder.struct().name("record").version(1);

        int count = 0;
        for (TypeDescription field: fields) {
            String type = field.getCategory().getName();

            switch (type) {
                case "boolean":
                    schemaBuilder = schemaBuilder.field(fieldNames.get(count), Schema.BOOLEAN_SCHEMA);
                    break;
                case "int":
                    schemaBuilder = schemaBuilder.field(fieldNames.get(count), Schema.INT32_SCHEMA);
                    break;
                case "bigint":
                    schemaBuilder = schemaBuilder.field(fieldNames.get(count), Schema.INT64_SCHEMA);
                    break;
                case "float":
                    schemaBuilder = schemaBuilder.field(fieldNames.get(count), Schema.FLOAT32_SCHEMA);
                    break;
                case "double":
                    schemaBuilder = schemaBuilder.field(fieldNames.get(count), Schema.FLOAT64_SCHEMA);
                    break;
            }
            count++;
        }
        return schemaBuilder.build();
    }

    private static TypeDescription convertKafkaToOrcSchema(Schema schema) {
        List<Field> fields = schema.fields();

        StringBuilder orcFields = new StringBuilder("struct<");

        int count = 0;
        for (Field field: fields) {
            String type = field.schema().type().getName();
            String name = field.name();

            switch (type) {
                case "int32":
                    orcFields.append(name + ":" + "int");
                    break;
                case "int64":
                    orcFields.append(name + ":" + "bigint");
                    break;
                case "float32":
                    orcFields.append(name + ":" + "float");
                    break;
                case "float64":
                    orcFields.append(name + ":" + "double");
                    break;
                default:
                    orcFields.append(name + ":" + type);
            }

            if (count < (fields.size() - 1)) {
                orcFields.append(",");
            }
            count++;
        }
        orcFields.append(">");
        return TypeDescription.fromString(orcFields.toString());
    }

    private static Object convertToTargetObject(Object o, Field field) {
        String type = field.schema().type().getName();
        switch (type) {
            case "boolean":
                return String.valueOf(o).equals("1");
            case "int32":
                return Integer.valueOf(String.valueOf(o));
            case "int64":
                return Long.valueOf(String.valueOf(o));
            case "float32":
                return Float.valueOf(String.valueOf(o));
            case "float64":
                return Double.valueOf(String.valueOf(o));
        }
        return null;
    }

    private static List<ColumnVector> getColumnVectors(VectorizedRowBatch rowBatch,
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

    private static void fillColumnVectors(GenericRecord currRecord,
                                   List<org.apache.avro.Schema.Field> fields,
                                   TypeDescription orcSchema, List<ColumnVector> columnVectors,
                                          int row) {
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

    public static byte[] putRecords(Collection<SinkRecord> records, AvroData avroData) throws IOException {
        Schema kafkaSchema = records.iterator().next().valueSchema();
        TypeDescription orcSchema = convertKafkaToOrcSchema(kafkaSchema);

        Path tempFilePath = new Path("temp");
        Writer orcWriter = OrcFile.createWriter(tempFilePath,
                OrcFile.writerOptions(new Configuration()).setSchema(orcSchema));
        VectorizedRowBatch rowBatch = orcSchema.createRowBatch();

        List<ColumnVector> columnVectors = getColumnVectors(rowBatch, orcSchema,
                orcSchema.getChildren().size());

        for (SinkRecord sinkRecord: records) {
            List<org.apache.avro.Schema.Field> fields =
                    avroData.fromConnectSchema(sinkRecord.valueSchema()).getFields();
            GenericRecord record =
                    (GenericRecord) avroData.fromConnectData(sinkRecord.valueSchema(), sinkRecord.value());

            int row = rowBatch.size++;
            fillColumnVectors(record, fields, orcSchema, columnVectors, row);
        }
        orcWriter.addRowBatch(rowBatch);
        rowBatch.reset();

        orcWriter.close();

        FSDataInputStream fsDataInputStream = tempFilePath
                .getFileSystem(new Configuration()).open(tempFilePath);
        InputStream inputStream = fsDataInputStream.getWrappedStream();
        fsDataInputStream.getWrappedStream();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buf = new byte[8000];
        int bytesRead;
        while((bytesRead = inputStream.read(buf)) != -1) {
            out.write(buf, 0, bytesRead);
        }
        fsDataInputStream.close();
        tempFilePath.getFileSystem(new Configuration()).delete(tempFilePath, false);
        return out.toByteArray();
    }
}