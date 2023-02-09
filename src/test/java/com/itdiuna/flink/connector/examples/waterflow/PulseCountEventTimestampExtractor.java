package com.itdiuna.flink.connector.examples.waterflow;

import com.itdiuna.flink.connector.mqtt.source.RecordTimestampExtractor;

public class PulseCountEventTimestampExtractor implements RecordTimestampExtractor<PulseCountEvent> {

    @Override
    public Long apply(PulseCountEvent pulseCountEvent) {
        return pulseCountEvent.getTimestamp().getTime();
    }
}
