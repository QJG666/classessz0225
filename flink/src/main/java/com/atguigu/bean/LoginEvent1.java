package com.atguigu.bean;

import java.util.Objects;

public class LoginEvent1 {

    private Long userId;
    private String ip;
    private String eventType;
    private Long eventTime;

    public LoginEvent1() {
    }

    public LoginEvent1(Long userId, String ip, String eventType, Long eventTime) {
        this.userId = userId;
        this.ip = ip;
        this.eventType = eventType;
        this.eventTime = eventTime;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
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
        LoginEvent1 that = (LoginEvent1) o;
        return Objects.equals(userId, that.userId) &&
                Objects.equals(ip, that.ip) &&
                Objects.equals(eventType, that.eventType) &&
                Objects.equals(eventTime, that.eventTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(userId, ip, eventType, eventTime);
    }

    @Override
    public String toString() {
        return "LoginEvent1(" +
                "userId=" + userId +
                ", ip='" + ip + '\'' +
                ", eventType='" + eventType + '\'' +
                ", eventTime=" + eventTime +
                ')';
    }
}
