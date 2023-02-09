package com.itdiuna.flink.connector.examples.waterflow;

import org.apache.commons.lang3.builder.ToStringBuilder;

import java.io.Serializable;
import java.util.Date;

public class PulseCountEvent implements Serializable {

    private String sensorId;

    private int count;

    private Date timestamp;

    public PulseCountEvent() {
    }

    public PulseCountEvent(String sensorId, int count, Date timestamp) {
        this.sensorId = sensorId;
        this.count = count;
        this.timestamp = timestamp;
    }

    public String getSensorId() {
        return sensorId;
    }

    public int getCount() {
        return count;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public void setSensorId(String sensorId) {
        this.sensorId = sensorId;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("sensor id", sensorId)
                .append("timestamp", timestamp)
                .append("count", count)
                .toString();
    }
}
