package com.atguigu.bean;

import java.util.Objects;

public class ItemCount1 {

    private Long item;
    private String time;
    private Integer count;

    public ItemCount1() {
    }

    public ItemCount1(Long item, String time, Integer count) {
        this.item = item;
        this.time = time;
        this.count = count;
    }

    public Long getItem() {
        return item;
    }

    public void setItem(Long item) {
        this.item = item;
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
        ItemCount1 that = (ItemCount1) o;
        return Objects.equals(item, that.item) &&
                Objects.equals(time, that.time) &&
                Objects.equals(count, that.count);
    }

    @Override
    public int hashCode() {
        return Objects.hash(item, time, count);
    }

    @Override
    public String toString() {
        return "ItemCount1{" +
                "item=" + item +
                ", time='" + time + '\'' +
                ", count=" + count +
                '}';
    }
}
