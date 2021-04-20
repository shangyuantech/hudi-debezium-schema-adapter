package org.apache.hudi.debezium.kafka.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.avro.AvroFactory;
import com.fasterxml.jackson.dataformat.avro.AvroSchema;
import com.fasterxml.jackson.dataformat.avro.schema.AvroSchemaGenerator;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class AvroUtils {

    private static final ObjectMapper mapper = new ObjectMapper(new AvroFactory());

    public static <T> T transformAvroToObject(GenericData.Record recordData, Class<T> clazz) throws IOException {
        //AvroSchema avroSchema = new AvroSchema(recordData.getSchema());
        SpecificDatumWriter<Object> writer = new SpecificDatumWriter<>(recordData.getSchema());
        byte[] serialized = null;

        try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            BinaryEncoder binaryEncoder = EncoderFactory.get().binaryEncoder(os, null);
            writer.write(recordData, binaryEncoder);
            binaryEncoder.flush();
            serialized = os.toByteArray();
        }

        //return mapper.reader(clazz).with(avroSchema).readValue(serialized);
        return transformByteToObject(serialized, clazz);
    }

    public static <T> T transformByteToObject(byte[] serialized, Class<T> clazz) throws IOException {
        return mapper.reader(clazz).with(new AvroSchema(getObjectSchema(clazz))).readValue(serialized);
    }

    public static <T> byte[] transformObjectToByte(T object, Class<T> clazz) throws JsonProcessingException {
        return mapper.writer(new AvroSchema(getObjectSchema(clazz)))
                .writeValueAsBytes(object);
    }

    public static <T> Schema getObjectSchema(Class<T> clazz) throws JsonMappingException {
        AvroSchemaGenerator gen = new AvroSchemaGenerator();
        mapper.acceptJsonFormatVisitor(clazz, gen);
        AvroSchema schemaWrapper = gen.getGeneratedSchema();
        return schemaWrapper.getAvroSchema();
    }

}
