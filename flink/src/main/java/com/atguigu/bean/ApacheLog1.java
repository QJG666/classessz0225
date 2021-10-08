package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Objects;


public class ApacheLog1 {

    private String ip;
    private String userId;
    private Long ts;
    private String method;
    private String url;

    public ApacheLog1() {
    }

    public ApacheLog1(String ip, String userId, Long ts, String method, String url) {
        this.ip = ip;
        this.userId = userId;
        this.ts = ts;
        this.method = method;
        this.url = url;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ApacheLog1 that = (ApacheLog1) o;
        return Objects.equals(ip, that.ip) &&
                Objects.equals(userId, that.userId) &&
                Objects.equals(ts, that.ts) &&
                Objects.equals(method, that.method) &&
                Objects.equals(url, that.url);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ip, userId, ts, method, url);
    }

    @Override
    public String toString() {
        return "ApacheLog1(" +
                "ip='" + ip + '\'' +
                ", userId='" + userId + '\'' +
                ", ts=" + ts +
                ", method='" + method + '\'' +
                ", url='" + url + '\'' +
                ')';
    }
}
