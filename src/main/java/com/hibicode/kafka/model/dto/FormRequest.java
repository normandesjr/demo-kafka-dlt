package com.hibicode.kafka.model.dto;

public class FormRequest {

    private String value;
    private ErrorType errorType;
    private RetryableStopAt retryableStopAt;

    public FormRequest() {
    }

    public FormRequest(ErrorType errorType, RetryableStopAt retryableStopAt) {
        this.errorType = errorType;
        this.retryableStopAt = retryableStopAt;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public ErrorType getErrorType() {
        return errorType;
    }

    public void setErrorType(ErrorType errorType) {
        this.errorType = errorType;
    }

    public RetryableStopAt getRetryableStopAt() {
        return retryableStopAt;
    }

    public void setRetryableStopAt(RetryableStopAt retryableStopAt) {
        this.retryableStopAt = retryableStopAt;
    }

    @Override
    public String toString() {
        return "FormRequest{" +
                "value='" + value + '\'' +
                ", errorType=" + errorType +
                ", retryableStopAt=" + retryableStopAt +
                '}';
    }
}
