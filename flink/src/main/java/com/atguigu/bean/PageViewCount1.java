package com.atguigu.bean;

import java.util.Objects;

public class PageViewCount1 {

    private String pv;
    private String time;
    private Integer count;

    public PageViewCount1() {
    }

    public PageViewCount1(String pv, String time, Integer count) {
        this.pv = pv;
        this.time = time;
        this.count = count;
    }

    public String getPv() {
        return pv;
    }

    public void setPv(String pv) {
        this.pv = pv;
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
        PageViewCount1 that = (PageViewCount1) o;
        return Objects.equals(pv, that.pv) &&
                Objects.equals(time, that.time) &&
                Objects.equals(count, that.count);
    }

    @Override
    public int hashCode() {
        return Objects.hash(pv, time, count);
    }

    @Override
    public String toString() {
        return "PageViewCount1{" +
                "pv='" + pv + '\'' +
                ", time='" + time + '\'' +
                ", count=" + count +
                '}';
    }
}
