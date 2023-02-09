package com.itdiuna.flink.connector.mqtt.source.enumerator;

import org.eclipse.paho.mqttv5.client.*;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.eclipse.paho.mqttv5.common.packet.MqttProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

public class TopicStatusListener {

    private final MqttClient client;

    private final String topicFilter;

    private final Set<String> discoveredTopics = new HashSet<>();

    private final Logger logger = LoggerFactory.getLogger(getClass());

    public TopicStatusListener(String serverURI, String topicFilter) {
        try {
            this.client = new MqttClient(serverURI, "flink");
        } catch (MqttException e) {
            throw new RuntimeException(e);
        }
        this.topicFilter = topicFilter;
    }

    public void start() {
        MqttConnectionOptions options = new MqttConnectionOptions();
        options.setKeepAliveInterval(1200);
        options.setCleanStart(false);

        try {
            client.setCallback(new MqttCallback() {
                @Override
                public void disconnected(MqttDisconnectResponse disconnectResponse) {
                    logger.warn("Client disconnected: {}", disconnectResponse);
                }

                @Override
                public void mqttErrorOccurred(MqttException exception) {
                    logger.warn("MQTT error occurred", exception);
                }

                @Override
                public void deliveryComplete(IMqttToken token) {
                    logger.warn("Delivery complete");
                }

                @Override
                public void connectComplete(boolean reconnect, String serverURI) {
                    logger.warn("Connection complete");
                }

                @Override
                public void authPacketArrived(int reasonCode, MqttProperties properties) {
                    logger.warn("AUTH packet arrived");
                }

                @Override
                public void messageArrived(String topic, MqttMessage mqttMessage) throws Exception {
                    String dataTopic = renameTopic(topic);

                    synchronized (discoveredTopics) {
                        discoveredTopics.add(dataTopic);
                    }
                }

                private String renameTopic(String topic) {
                    return topic.replaceAll("status-up$", "data");
                }
            });
            client.connect(options);

            client.subscribe(topicFilter, 1);
        } catch (MqttException e) {
            throw new RuntimeException(e);
        }
    }

    public Set<String> getDiscoveredTopics() {
        synchronized (discoveredTopics) {
            return new HashSet<>(discoveredTopics);
        }
    }
}
