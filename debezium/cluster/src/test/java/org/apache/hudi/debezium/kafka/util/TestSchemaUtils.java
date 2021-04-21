package org.apache.hudi.debezium.kafka.util;

import org.apache.avro.generic.GenericRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import org.apache.hudi.debezium.example.Employee;

public class TestSchemaUtils {

//    private String SCHEMA_JSON = "{\n"
//            +"\"type\": \"record\",\n"
//            +"\"name\": \"Employee\",\n"
//            +"\"fields\": [\n"
//            +" {\"name\": \"name\", \"type\": \"string\"},\n"
//            +" {\"name\": \"age\", \"type\": \"int\"},\n"
//            +" {\"name\": \"emails\", \"type\": {\"type\": \"array\", \"items\": \"string\"}},\n"
//            +" {\"name\": \"boss\", \"type\": [\"Employee\",\"null\"]}\n"
//            +"]}";

    private Employee employee;

    @Before
    public void setUp() {
        employee = new Employee();
        employee.setName("mike");
        employee.setAge(31);
        employee.setEmails("mike@hotmail.com");
    }

    @Test
    public void testAvroData() throws IOException {
        byte[] avroData = AvroUtils.transformObjectToByte(employee, Employee.class);
        Employee empl = AvroUtils.transformByteToObject(avroData, Employee.class);
        Assert.assertEquals(empl.getName(), employee.getName());
    }

    @Test
    public void testObjectToAvro() throws IOException {
        GenericRecord genericRecord = AvroUtils.transformObjectToAvro(employee, Employee.class);
        Assert.assertEquals(genericRecord.get("name").toString(), employee.getName());
        Assert.assertEquals(genericRecord.get("age"), employee.getAge());
    }

}
