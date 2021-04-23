package org.apache.hudi.debezium.common.exception;

public class SlaveTaskNotFoundException extends Exception {

    private static final long serialVersionUID = 1L;

    public SlaveTaskNotFoundException() { }

    public SlaveTaskNotFoundException(String message) {
        super(message);
    }

}

