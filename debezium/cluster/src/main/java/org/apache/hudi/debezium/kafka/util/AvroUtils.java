package org.apache.hudi.debezium.kafka.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.avro.AvroFactory;
import com.fasterxml.jackson.dataformat.avro.AvroSchema;
import com.fasterxml.jackson.dataformat.avro.schema.AvroSchemaGenerator;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;

import javax.validation.constraints.NotNull;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;

/**
 * use https://github.com/FasterXML/jackson-dataformats-binary/tree/master/avro
 */
public class AvroUtils {

    private static final ObjectMapper mapper = new ObjectMapper(new AvroFactory())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);;

    public static <T> T transformAvroToObject(@NotNull GenericRecord recordData, Class<T> clazz) throws IOException {
        SpecificDatumWriter<Object> writer = new SpecificDatumWriter<>(recordData.getSchema());
        byte[] serialized = null;

        try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            BinaryEncoder binaryEncoder = EncoderFactory.get().binaryEncoder(os, null);
            writer.write(recordData, binaryEncoder);
            binaryEncoder.flush();
            serialized = os.toByteArray();
        }

        return transformByteToObject(serialized, clazz);
    }

    public static <T> T transformByteToObject(byte[] serialized, Class<T> clazz) throws IOException {
        return mapper.reader(clazz).with(new AvroSchema(getObjectSchema(clazz))).readValue(serialized);
    }

    public static <T> byte[] transformObjectToByte(T object, Class<T> clazz) throws JsonProcessingException {
        return mapper.writer(new AvroSchema(getObjectSchema(clazz)))
                .writeValueAsBytes(object);
    }

    public static <T> GenericRecord transformObjectToAvro(T object, Class<T> clazz) throws IOException {
        GenericDatumReader<T> genericRecordReader = new GenericDatumReader<>(getObjectSchema(clazz));
        return (GenericRecord) genericRecordReader.read(null,
                DecoderFactory.get().binaryDecoder(transformObjectToByte(object, clazz), null));
    }

    public static <T> Schema getObjectSchema(Class<T> clazz) throws JsonMappingException {
        AvroSchemaGenerator gen = new AvroSchemaGenerator();
        mapper.acceptJsonFormatVisitor(clazz, gen);
        AvroSchema schemaWrapper = gen.getGeneratedSchema();
        return schemaWrapper.getAvroSchema();
    }

    public static Schema getAvroSchemaByFile(String fileName) throws IOException {
        File schemaFile = new File(fileName);
        Schema.Parser parser = new Schema.Parser();
        return parser.parse(schemaFile);
    }

}
