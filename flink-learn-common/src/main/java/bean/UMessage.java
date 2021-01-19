package bean;

import lombok.Builder;
import lombok.Data;

/**
 * @Author wangwenbo
 * @Date 2021/1/19 11:20 下午
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
