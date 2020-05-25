package com.hibicode.kafka.model.dto;

public enum RetryableStopAt {

    DLT("0"),
    RETRY_1("1"),
    RETRY_2("2"),
    RETRY_3("3");

    private String index;

    RetryableStopAt(String index) {
        this.index = index;
    }

    public String getIndex() {
        return index;
    }
}
