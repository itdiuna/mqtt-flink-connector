package com.itdiuna.flink.connector.mqtt.source.split;

import java.util.LinkedList;
import java.util.List;

public class MqttTopicSplitState {

    private final List<Integer> processedMessages = new LinkedList<>();

    public void process(int messageId) {
        processedMessages.add(messageId);
    }

    public List<Integer> streamProcessedMessages() {
        return processedMessages;
    }
}
