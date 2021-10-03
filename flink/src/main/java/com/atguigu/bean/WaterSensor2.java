package com.atguigu.bean;

import java.util.Objects;

public class WaterSensor2 {

    private String id;
    private Long ts;
    private Double vc;

    public WaterSensor2() {
    }

    public WaterSensor2(String id, Long ts, Double vc) {
        this.id = id;
        this.ts = ts;
        this.vc = vc;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    public Double getVc() {
        return vc;
    }

    public void setVc(Double vc) {
        this.vc = vc;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WaterSensor2 that = (WaterSensor2) o;
        return Objects.equals(id, that.id) &&
                Objects.equals(ts, that.ts) &&
                Objects.equals(vc, that.vc);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, ts, vc);
    }

    @Override
    public String toString() {
        return "WaterSensor2{" +
                "id='" + id + '\'' +
                ", ts=" + ts +
                ", vc=" + vc +
                '}';
    }
}
