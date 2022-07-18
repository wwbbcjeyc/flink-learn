package com.wwb.watermark;

import com.wwb.bean.Event;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

/**
 * @Author wangwenbo
 * @Date 2022/5/4 23:14
 * @Version 1.0
 */
public class CustomPunctuatedGenerator implements WatermarkGenerator<Event> {

    @Override
    public void onEvent(Event event, long eventTimestamp, WatermarkOutput output) {
        // 只有在遇到特定的 itemId 时，才发出水位线
        if (event.user.equals("Mary")) {
            output.emitWatermark(new Watermark(event.timestamp - 1));
        }
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        // 不需要做任何事情，因为我们在 onEvent 方法中发射了水位线
    }
}
