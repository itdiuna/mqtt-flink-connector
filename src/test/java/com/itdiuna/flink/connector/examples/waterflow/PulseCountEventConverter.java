package com.itdiuna.flink.connector.examples.waterflow;

import com.itdiuna.flink.connector.mqtt.source.MqttMessagePayloadConverter;
import org.eclipse.paho.mqttv5.common.MqttMessage;

import java.util.Date;

public class PulseCountEventConverter implements MqttMessagePayloadConverter<PulseCountEvent> {
    @Override
    public PulseCountEvent apply(MqttMessage mqttMessage) {
        byte[] payload = mqttMessage.getPayload();
        long timestamp = 0;
        for (int i = 7; i >= 0; --i) {
            timestamp = (timestamp << 8) + (payload[i] & 255);
        }
        int count = 0;
        for (int i = 8; i < 12; ++i) {
            count = (count << 8) + (payload[i] & 255);
        }
        return new PulseCountEvent("dummy", count, new Date(timestamp * 1_000));
    }
}
