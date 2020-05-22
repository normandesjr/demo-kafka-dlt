package com.hibicode.kafka.consumer.model;

public class RechargeRequest {

    private String account;

    public String getAccount() {
        return account;
    }

    public void setAccount(String account) {
        this.account = account;
    }

    @Override
    public String toString() {
        return "RechargeRequest{" +
                "account='" + account + '\'' +
                '}';
    }
}
