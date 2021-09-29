package com.atguigu.bean;

import java.util.Objects;

public class OrderEvent1 {
    private Long orderId;
    private String eventType;
    private String txId;
    private Long eventTime;

    public OrderEvent1() {
    }

    public OrderEvent1(Long orderId, String eventType, String txId, Long eventTime) {
        this.orderId = orderId;
        this.eventType = eventType;
        this.txId = txId;
        this.eventTime = eventTime;
    }

    public Long getOrderId() {
        return orderId;
    }

    public void setOrderId(Long orderId) {
        this.orderId = orderId;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public String getTxId() {
        return txId;
    }

    public void setTxId(String txId) {
        this.txId = txId;
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
        OrderEvent1 that = (OrderEvent1) o;
        return Objects.equals(orderId, that.orderId) &&
                Objects.equals(eventType, that.eventType) &&
                Objects.equals(txId, that.txId) &&
                Objects.equals(eventTime, that.eventTime);
    }

    @Override
    public int hashCode() {

        return Objects.hash(orderId, eventType, txId, eventTime);
    }

    @Override
    public String toString() {
        return "OrderEvent1{" +
                "orderId=" + orderId +
                ", eventType='" + eventType + '\'' +
                ", txId='" + txId + '\'' +
                ", eventTime=" + eventTime +
                '}';
    }
}

