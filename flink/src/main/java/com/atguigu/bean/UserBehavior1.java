package com.atguigu.bean;

import java.util.Objects;

public class UserBehavior1 {
    private Long userId;
    private Long itemId;
    private Integer categoryId;
    private String behavior;
    private Long timestamp;

    public UserBehavior1() {
    }

    public UserBehavior1(Long userId, Long itemId, Integer categoryId, String behavior, Long timestamp) {
        this.userId = userId;
        this.itemId = itemId;
        this.categoryId = categoryId;
        this.behavior = behavior;
        this.timestamp = timestamp;
    }

    public Long getUserId() {
        return userId;
    }

    public Long getItemId() {
        return itemId;
    }

    public Integer getCategoryId() {
        return categoryId;
    }

    public String getBehavior() {
        return behavior;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public void setItemId(Long itemId) {
        this.itemId = itemId;
    }

    public void setCategoryId(Integer categoryId) {
        this.categoryId = categoryId;
    }

    public void setBehavior(String behavior) {
        this.behavior = behavior;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UserBehavior1 that = (UserBehavior1) o;
        return Objects.equals(userId, that.userId) &&
                Objects.equals(itemId, that.itemId) &&
                Objects.equals(categoryId, that.categoryId) &&
                Objects.equals(behavior, that.behavior) &&
                Objects.equals(timestamp, that.timestamp);
    }

    @Override
    public int hashCode() {

        return Objects.hash(userId, itemId, categoryId, behavior, timestamp);
    }

    @Override
    public String toString() {
        return "UserBehavior1{" +
                "userId=" + userId +
                ", itemId=" + itemId +
                ", categoryId=" + categoryId +
                ", behavior='" + behavior + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
