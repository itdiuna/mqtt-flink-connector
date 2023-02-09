package com.itdiuna.flink.connector.mqtt.source;

import java.io.Serializable;
import java.util.function.Function;

public interface RecordTimestampExtractor<T> extends Function<T, Long>, Serializable {
}
