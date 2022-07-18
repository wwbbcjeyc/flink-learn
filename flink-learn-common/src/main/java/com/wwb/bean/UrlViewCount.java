package com.wwb.bean;

import java.sql.Timestamp;

/**
 * @Author wangwenbo
 * @Date 2022/5/2 23:07
 * @Version 1.0
 */
public class UrlViewCount {
    public String url; //url
    public Long count; //数量
    public Long windowStart; //开始时间
    public Long windowEnd; //结束时间

    public UrlViewCount() {
    }

    public UrlViewCount(String url, Long count, Long windowStart, Long windowEnd) {
        this.url = url;
        this.count = count;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
    }

    @Override
    public String toString() {
        return "UrlViewCount[" +
                "url='" + url + '\'' +
                ", count=" + count +
                ", windowStart=" + new Timestamp(windowStart) +
                ", windowEnd=" + new Timestamp(windowEnd) +
                ']';
    }
}

