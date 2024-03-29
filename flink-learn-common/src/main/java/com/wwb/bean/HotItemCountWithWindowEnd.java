package com.wwb.bean;

/**
 * @Author wangwenbo
 * @Date 2022/5/3 10:48
 * @Version 1.0
 */
public class HotItemCountWithWindowEnd implements Comparable<HotItemCountWithWindowEnd>{

    private Long itemId;
    private Long itemCount;
    private Long windowEnd;

    public HotItemCountWithWindowEnd() {
    }

    public HotItemCountWithWindowEnd(Long itemId, Long itemCount, Long windowEnd) {
        this.itemId = itemId;
        this.itemCount = itemCount;
        this.windowEnd = windowEnd;
    }

    public Long getItemId() {
        return itemId;
    }

    public void setItemId(Long itemId) {
        this.itemId = itemId;
    }

    public Long getItemCount() {
        return itemCount;
    }

    public void setItemCount(Long itemCount) {
        this.itemCount = itemCount;
    }

    public Long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(Long windowEnd) {
        this.windowEnd = windowEnd;
    }

    @Override
    public String toString() {
        return "HotItemCountWithWindowEnd{" +
                "itemId=" + itemId +
                ", itemCount=" + itemCount +
                ", windowEnd=" + windowEnd +
                '}';
    }

    @Override
    public int compareTo(HotItemCountWithWindowEnd o) {
        // 后减前 降序
        return o.getItemCount().intValue() - this.getItemCount().intValue();
    }
}

