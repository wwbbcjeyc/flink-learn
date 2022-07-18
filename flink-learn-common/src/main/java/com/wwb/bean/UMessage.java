package com.wwb.bean;

import lombok.Builder;
import lombok.Data;

/**
 * @Author wangwenbo
 * @Date 2022/5/3 10:52
 * @Version 1.0
 */
@Data
@Builder
public class UMessage {

    private String uid;

    private String createTime;

    public UMessage() {
    }

    public UMessage(String uid, String createTime) {
        this.uid = uid;
        this.createTime = createTime;
    }
}
