package com.itdiuna.flink.connector.mqtt.source.split;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.eclipse.paho.mqttv5.client.*;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.eclipse.paho.mqttv5.common.packet.MqttProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;

public class MqttTopicSplitReader implements SplitReader<MqttMessage, MqttTopicSplit> {

    private final MqttClient client;

    private final List<TopicMessage> receivedMessages = new LinkedList<>();

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final Set<MqttTopicSplit> awaitingSplits = new HashSet<>();

    public MqttTopicSplitReader(SourceReaderContext context, String brokerURI) {
        try {
            client = new MqttClient(
                    brokerURI,
                    "flink-reader"
            );
            client.setManualAcks(true);
            client.setCallback(new Callback());
            MqttConnectionOptions options = new MqttConnectionOptions();
            options.setCleanStart(false);
            options.setKeepAliveInterval(1200);
            logger.info("Connecting to the broker now...");
            client.connect(options);
            logger.info("Connection should be initialized now...");
        } catch (MqttException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public RecordsWithSplitIds<MqttMessage> fetch() throws IOException {
        synchronized (receivedMessages) {
            TopicSplitRecords topicSplitRecords = new TopicSplitRecords(receivedMessages);
            receivedMessages.clear();
            return topicSplitRecords;
        }
    }

    @Override
    public void handleSplitsChanges(SplitsChange<MqttTopicSplit> splitsChanges) {
        synchronized (awaitingSplits) {
            if (client.isConnected()) {
                subscribeToTopics(splitsChanges.splits());
            } else {
                awaitingSplits.addAll(splitsChanges.splits());
            }
        }
    }

    private void subscribeToTopics(Collection<MqttTopicSplit> splits) {
        splits.forEach(
                split -> {
                    try {
                        client.subscribe(split.splitId(), 1);
                        logger.info("Subscribed to topic: {}", split.splitId());
                    } catch (MqttException e) {
                        throw new RuntimeException(e);
                    }
                }
        );
    }

    @Override
    public void wakeUp() {

    }

    @Override
    public void close() throws Exception {
        if (client != null) {
            client.close();
        }
    }

    public void acknowledge(List<Integer> processedMessages) {
        processedMessages
                .forEach(message -> {
                    try {
                        logger.info("Sending acknowledgment to message id: {}", message);
                        client.messageArrivedComplete(message, 1);
                    } catch (MqttException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    private static class TopicSplitRecords implements RecordsWithSplitIds<MqttMessage> {

        private final Iterator<Map.Entry<String, Collection<MqttMessage>>> splitIterator;

        private Iterator<MqttMessage> messageIterator;

        public TopicSplitRecords(List<TopicMessage> arrivedMessages) {
            Map<String, Collection<MqttMessage>> splits = new HashMap<>();
            for (TopicMessage topicMessage : arrivedMessages) {
                splits.computeIfAbsent(
                        topicMessage.topic,
                        (topic) -> new LinkedList<>()
                );
                splits.get(topicMessage.topic).add(topicMessage.message);
            }
            splitIterator = splits.entrySet().iterator();
        }

        @Nullable
        @Override
        public String nextSplit() {
            if (splitIterator.hasNext()) {
                Map.Entry<String, Collection<MqttMessage>> split = splitIterator.next();
                messageIterator = split.getValue().iterator();
                return split.getKey();
            } else {
                return null;
            }
        }

        @Nullable
        @Override
        public MqttMessage nextRecordFromSplit() {
            if (messageIterator.hasNext()) {
                return messageIterator.next();
            }
            return null;
        }

        @Override
        public Set<String> finishedSplits() {
            return Collections.emptySet();
        }
    }

    private class Callback implements MqttCallback {

        @Override
        public void disconnected(MqttDisconnectResponse disconnectResponse) {

        }

        @Override
        public void mqttErrorOccurred(MqttException exception) {

        }

        @Override
        public void messageArrived(String topic, MqttMessage message) throws Exception {
            logger.warn("arrived message: {}", message.getId());
            synchronized (receivedMessages) {
                receivedMessages.add(new TopicMessage(topic, message));
            }
        }

        @Override
        public void deliveryComplete(IMqttToken token) {

        }

        @Override
        public void connectComplete(boolean reconnect, String serverURI) {
            logger.info("Connected");
            synchronized (awaitingSplits) {
                subscribeToTopics(awaitingSplits);
                awaitingSplits.clear();
            }
        }

        @Override
        public void authPacketArrived(int reasonCode, MqttProperties properties) {

        }
    }

    private static class TopicMessage {
        private final String topic;

        private final MqttMessage message;

        private TopicMessage(String topic, MqttMessage message) {
            this.topic = topic;
            this.message = message;
        }
    }
}
