package com.atguigu.bean;

import java.util.Objects;

public class TxEvent1 {
    private String txId;
    private String payChannel;
    private Long eventTime;

    public TxEvent1() {
    }

    public TxEvent1(String txId, String payChannel, Long eventTime) {
        this.txId = txId;
        this.payChannel = payChannel;
        this.eventTime = eventTime;
    }

    public String getTxId() {
        return txId;
    }

    public void setTxId(String txId) {
        this.txId = txId;
    }

    public String getPayChannel() {
        return payChannel;
    }

    public void setPayChannel(String payChannel) {
        this.payChannel = payChannel;
    }

    public Long getEventTime() {
        return eventTime;
    }

    public void setEventTime(Long eventTime) {
        this.eventTime = eventTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TxEvent1 txEvent1 = (TxEvent1) o;
        return Objects.equals(txId, txEvent1.txId) &&
                Objects.equals(payChannel, txEvent1.payChannel) &&
                Objects.equals(eventTime, txEvent1.eventTime);
    }

    @Override
    public int hashCode() {

        return Objects.hash(txId, payChannel, eventTime);
    }

    @Override
    public String toString() {
        return "TxEvent1{" +
                "txId='" + txId + '\'' +
                ", payChannel='" + payChannel + '\'' +
                ", eventTime=" + eventTime +
                '}';
    }
}

