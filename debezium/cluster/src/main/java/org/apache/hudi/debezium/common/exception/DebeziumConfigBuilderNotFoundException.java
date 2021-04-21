package org.apache.hudi.debezium.common.exception;

public class DebeziumConfigBuilderNotFoundException extends Exception {

    private static final long serialVersionUID = 1L;

    public DebeziumConfigBuilderNotFoundException() { }

    public DebeziumConfigBuilderNotFoundException(String message) {
        super(message);
    }

}

