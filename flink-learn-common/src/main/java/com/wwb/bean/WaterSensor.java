package com.wwb.bean;

/**
 * @Author wangwenbo
 * @Date 2022/5/2 22:19
 * @Version 1.0
 */
public class WaterSensor {
    //传感器ID
    private String id;
    //时间戳
    private Long ts;
    //空高，这里为了演示方便，认为是 水位高度
    private Integer vc;

    public WaterSensor() {
    }

    public WaterSensor(String id, Long ts, Integer vc) {
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

    @Override
    public String toString() {
        return "WaterSensor{" +
                "id='" + id + '\'' +
                ", ts=" + ts +
                ", vc=" + vc +
                '}';
    }
}

