package org.apache.hudi.debezium.common.exception;

public class DebeziumTaskNotFoundException extends Exception {

    private static final long serialVersionUID = 1L;

    public DebeziumTaskNotFoundException() { }

    public DebeziumTaskNotFoundException(String message) {
        super(message);
    }

}

