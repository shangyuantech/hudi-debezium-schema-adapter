package org.apache.hudi.debezium.common.exception;

public class DebeziumConfigNotFoundException extends Exception {

    private static final long serialVersionUID = 1L;

    public DebeziumConfigNotFoundException() { }

    public DebeziumConfigNotFoundException(String message) {
        super(message);
    }

}

