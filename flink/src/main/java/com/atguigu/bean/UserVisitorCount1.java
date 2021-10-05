package com.atguigu.bean;

import java.util.Objects;

public class UserVisitorCount1 {

    private String uv;
    private String time;
    private Integer count;

    public UserVisitorCount1() {
    }

    public UserVisitorCount1(String uv, String time, Integer count) {
        this.uv = uv;
        this.time = time;
        this.count = count;
    }

    public String getUv() {
        return uv;
    }

    public void setUv(String uv) {
        this.uv = uv;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UserVisitorCount1 that = (UserVisitorCount1) o;
        return Objects.equals(uv, that.uv) &&
                Objects.equals(time, that.time) &&
                Objects.equals(count, that.count);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uv, time, count);
    }

    @Override
    public String toString() {
        return "UserVisitorCount1{" +
                "uv='" + uv + '\'' +
                ", time='" + time + '\'' +
                ", count=" + count +
                '}';
    }
}
