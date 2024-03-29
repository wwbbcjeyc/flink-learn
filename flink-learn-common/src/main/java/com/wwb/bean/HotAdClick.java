package com.wwb.bean;

/**
 * @Author wangwenbo
 * @Date 2022/5/3 10:48
 * @Version 1.0
 */
public class HotAdClick {
    private String province;
    private Long adId;
    private Long clickCount;
    private Long windowEnd;

    public HotAdClick() {
    }

    public HotAdClick(String province, Long adId, Long clickCount, Long windowEnd) {
        this.province = province;
        this.adId = adId;
        this.clickCount = clickCount;
        this.windowEnd = windowEnd;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public Long getAdId() {
        return adId;
    }

    public void setAdId(Long adId) {
        this.adId = adId;
    }

    public Long getClickCount() {
        return clickCount;
    }

    public void setClickCount(Long clickCount) {
        this.clickCount = clickCount;
    }

    public Long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(Long windowEnd) {
        this.windowEnd = windowEnd;
    }

    @Override
    public String toString() {
        return "HotAdClick{" +
                "province='" + province + '\'' +
                ", adId=" + adId +
                ", clickCount=" + clickCount +
                ", windowEnd=" + windowEnd +
                '}';
    }
}

