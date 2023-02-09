package com.itdiuna.flink.connector.mqtt.source.split;

import org.apache.flink.api.connector.source.SourceSplit;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

public class MqttTopicSplit implements SourceSplit, Serializable {

    private final String topicName;

    private final List<Integer> processedMessages;

    public MqttTopicSplit(String topicName) {
        this(topicName, Collections.emptyList());
    }

    public MqttTopicSplit(String topicName, List<Integer> processedMessages) {
        this.topicName = topicName;
        this.processedMessages = processedMessages;
    }

    public List<Integer> getProcessedMessages() {
        return processedMessages;
    }

    @Override
    public String splitId() {
        return topicName;
    }

    public void clearProcessedMessages() {
        processedMessages.clear();
    }
}
