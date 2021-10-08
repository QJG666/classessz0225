package com.atguigu.bean;

import java.util.Objects;

public class UrlCount1 {

    private String url;
    private Long windowEnd;
    private Integer count;

    public UrlCount1() {
    }

    public UrlCount1(String url, Long windowEnd, Integer count) {
        this.url = url;
        this.windowEnd = windowEnd;
        this.count = count;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(Long windowEnd) {
        this.windowEnd = windowEnd;
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
        UrlCount1 urlCount1 = (UrlCount1) o;
        return Objects.equals(url, urlCount1.url) &&
                Objects.equals(windowEnd, urlCount1.windowEnd) &&
                Objects.equals(count, urlCount1.count);
    }

    @Override
    public int hashCode() {
        return Objects.hash(url, windowEnd, count);
    }

    @Override
    public String toString() {
        return "UrlCount1(" +
                "url='" + url + '\'' +
                ", windowEnd=" + windowEnd +
                ", count=" + count +
                ')';
    }
}
