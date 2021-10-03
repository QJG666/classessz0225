package com.atguigu.bean;

import java.util.Objects;

public class AvgVc1 {

    private Integer vcSum;
    private Integer count;

    public AvgVc1() {
    }

    public AvgVc1(Integer vcSum, Integer count) {
        this.vcSum = vcSum;
        this.count = count;
    }

    public Integer getVcSum() {
        return vcSum;
    }

    public void setVcSum(Integer vcSum) {
        this.vcSum = vcSum;
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
        AvgVc1 avgVc1 = (AvgVc1) o;
        return Objects.equals(vcSum, avgVc1.vcSum) &&
                Objects.equals(count, avgVc1.count);
    }

    @Override
    public int hashCode() {
        return Objects.hash(vcSum, count);
    }

    @Override
    public String toString() {
        return "AvgVc1{" +
                "vcSum=" + vcSum +
                ", count=" + count +
                '}';
    }
}
