package bean;

import lombok.Data;

/**
 * 传感器温度读数的数据类型
 * @Author wangwenbo
 * @Date 2021/1/19 11:24 下午
 * @Version 1.0
 */

@Data
public class SensorReading {
    // 属性：id，时间戳，温度值
    private String id;
    private String name;
    private Long timestamp;
    private Double temperature;

    public SensorReading() {
    }

    public SensorReading(String id, Long timestamp, Double temperature) {
        this.id = id;
        this.timestamp = timestamp;
        this.temperature = temperature;
    }

    public SensorReading(String id, String name, Long timestamp, Double temperature) {
        this.id = id;
        this.name = name;
        this.timestamp = timestamp;
        this.temperature = temperature;
    }

    @Override
    public String toString() {
        return "SensorReading{" +
                "id='" + id + '\'' +
                ", timestamp=" + timestamp +
                ", temperature=" + temperature +
                '}';
    }
}
