package com.zjtd.dp.sink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.util.Preconditions;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * @Author Wang wenbo
 * @Date 2021/2/4 17:53
 * @Version 1.0
 */
public class EventTimeBucketAssigner<IN> implements BucketAssigner<IN, String> {

    private static final long serialVersionUID = 1L;
    private static final String DEFAULT_FORMAT_STRING = "yyyy-MM-dd HH";
    private final String formatString;
    private final ZoneId zoneId;
    private transient DateTimeFormatter dateTimeFormatter;

    public EventTimeBucketAssigner() {
        this("yyyy-MM-dd HH");
    }

    public EventTimeBucketAssigner(String formatString) {
        this(formatString, ZoneId.systemDefault());
    }

    public EventTimeBucketAssigner(ZoneId zoneId) {
        this("yyyy-MM-dd HH", zoneId);
    }

    public EventTimeBucketAssigner(String formatString, ZoneId zoneId) {
        this.formatString = (String) Preconditions.checkNotNull(formatString);
        this.zoneId = (ZoneId)Preconditions.checkNotNull(zoneId);
    }

    @Override
    public String getBucketId(IN element, Context context) {


        if (dateTimeFormatter == null) {
            dateTimeFormatter = DateTimeFormatter.ofPattern(formatString).withZone(zoneId);
        }
        JSONObject jsonObject = JSON.parseObject(element.toString());


        LocalDate pDay = LocalDate.from(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").parse(jsonObject.getJSONObject("common").getString("ds")));

        int pHour = LocalTime.from(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").parse(jsonObject.getJSONObject("common").getString("ds"))).getHour();


        String eventCode = jsonObject.getJSONArray("events").getJSONObject(0).getString("event_code");
        String product = jsonObject.getJSONObject("common").getString("product_name");
        String pDays = pDay.toString();
        String pHours = String.valueOf(pHour);


        return eventCode + "/p_product=" + product + "/p_Days=" + pDays + "/p_hours=" + pHours;

    }

    @Override
    public SimpleVersionedSerializer<String> getSerializer() {
        return  SimpleVersionedStringSerializer.INSTANCE;
    }

}
