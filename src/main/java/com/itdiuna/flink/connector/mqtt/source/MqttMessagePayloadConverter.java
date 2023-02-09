package com.itdiuna.flink.connector.mqtt.source;

import org.eclipse.paho.mqttv5.common.MqttMessage;

import java.io.Serializable;
import java.util.function.Function;

public interface MqttMessagePayloadConverter<T> extends Serializable, Function<MqttMessage, T> {
}
