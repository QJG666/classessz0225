package com.atguigu.bean;

import java.util.Objects;

public class MarketingUserBehavior1 {
    private Long userId;
    private String behavior;
    private String channel;
    private Long timestamp;

    public MarketingUserBehavior1() {
    }

    public MarketingUserBehavior1(Long userId, String behavior, String channel, Long timestamp) {
        this.userId = userId;
        this.behavior = behavior;
        this.channel = channel;
        this.timestamp = timestamp;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public String getBehavior() {
        return behavior;
    }

    public void setBehavior(String behavior) {
        this.behavior = behavior;
    }

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MarketingUserBehavior1 that = (MarketingUserBehavior1) o;
        return Objects.equals(userId, that.userId) &&
                Objects.equals(behavior, that.behavior) &&
                Objects.equals(channel, that.channel) &&
                Objects.equals(timestamp, that.timestamp);
    }

    @Override
    public int hashCode() {

        return Objects.hash(userId, behavior, channel, timestamp);
    }

    @Override
    public String toString() {
        return "MarketingUserBehavior1{" +
                "userId=" + userId +
                ", behavior='" + behavior + '\'' +
                ", channel='" + channel + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}

