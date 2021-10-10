package com.atguigu.bean;

import java.util.Objects;

public class AdCount1 {
    private String province;
    private Long windowEnd;
    private Integer count;

    public AdCount1() {
    }

    public AdCount1(String province, Long windowEnd, Integer count) {
        this.province = province;
        this.windowEnd = windowEnd;
        this.count = count;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
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
        AdCount1 adCount1 = (AdCount1) o;
        return Objects.equals(province, adCount1.province) &&
                Objects.equals(windowEnd, adCount1.windowEnd) &&
                Objects.equals(count, adCount1.count);
    }

    @Override
    public int hashCode() {
        return Objects.hash(province, windowEnd, count);
    }

    @Override
    public String toString() {
        return "AdCount1(" +
                "province='" + province + '\'' +
                ", windowEnd=" + windowEnd +
                ", count=" + count +
                ')';
    }
}
