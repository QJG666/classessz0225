package com.atguigu.bean;

import java.util.Objects;

public class WaterSensor1 {
    private String id;
    private Long ts;
    private Integer vc;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WaterSensor1 that = (WaterSensor1) o;
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
        return "WaterSensor1(" +
                "id=" + id  +
                ", ts=" + ts +
                ", vc=" + vc +
                ')';
    }

    public WaterSensor1() {
    }

    public WaterSensor1(String id, Long ts, Integer vc) {
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

    public Integer getVc() {
        return vc;
    }

    public void setVc(Integer vc) {
        this.vc = vc;
    }
}
