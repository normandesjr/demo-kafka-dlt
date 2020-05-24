package com.hibicode.kafka.exception;

public class RetryableException extends RuntimeException {

    public RetryableException(String message) {
        super(message);
    }
}
