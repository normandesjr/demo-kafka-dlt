package com.hibicode.kafka.exception;

public class NotRetryableException extends RuntimeException {

    public NotRetryableException(String message) {
        super(message);
    }
}
