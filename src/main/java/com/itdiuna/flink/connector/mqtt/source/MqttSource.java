package com.itdiuna.flink.connector.mqtt.source;

import com.itdiuna.flink.connector.mqtt.source.enumerator.MqttSourceEnumerator;
import com.itdiuna.flink.connector.mqtt.source.enumerator.MqttSourceEnumeratorState;
import com.itdiuna.flink.connector.mqtt.source.reader.MqttSourceReader;
import com.itdiuna.flink.connector.mqtt.source.split.MqttTopicSplit;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

public class MqttSource<T> implements Source<T, MqttTopicSplit, MqttSourceEnumeratorState>, Serializable {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final Function<MqttMessage, T> mapper;

    private final Function<T, Long> timestampExtractor;
    private final String brokerURI;
    private final String topicFilter;

    public MqttSource(MqttMessagePayloadConverter<T> converter, RecordTimestampExtractor<T> timestampExtractor, String brokerURI, String topicFilter) {
        this.mapper = converter;
        this.timestampExtractor = timestampExtractor;
        this.brokerURI = brokerURI;
        this.topicFilter = topicFilter;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<T, MqttTopicSplit> createReader(SourceReaderContext sourceReaderContext) throws Exception {
        logger.info("Creating a reader...");
        return new MqttSourceReader<>(sourceReaderContext, new FutureCompletingBlockingQueue<>(), mapper, timestampExtractor, brokerURI);
    }

    @Override
    public SplitEnumerator<MqttTopicSplit, MqttSourceEnumeratorState> createEnumerator(SplitEnumeratorContext<MqttTopicSplit> splitEnumeratorContext) throws Exception {
        return new MqttSourceEnumerator(splitEnumeratorContext, brokerURI, topicFilter);
    }

    @Override
    public SplitEnumerator<MqttTopicSplit, MqttSourceEnumeratorState> restoreEnumerator(SplitEnumeratorContext<MqttTopicSplit> splitEnumeratorContext, MqttSourceEnumeratorState checkpoint) throws Exception {
        return new MqttSourceEnumerator(splitEnumeratorContext, checkpoint.getAssignedTopics(), checkpoint.getUnassignedTopics(), brokerURI, topicFilter);
    }

    @Override
    public SimpleVersionedSerializer<MqttTopicSplit> getSplitSerializer() {
        return new SimpleVersionedSerializer<MqttTopicSplit>() {
            @Override
            public int getVersion() {
                return 0;
            }

            @Override
            public byte[] serialize(MqttTopicSplit topicSplit) throws IOException {
                try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                     DataOutputStream out = new DataOutputStream(baos)) {
                    out.writeUTF(topicSplit.splitId());
                    for (Integer message : topicSplit.getProcessedMessages()) {
                        out.writeInt(message);
                    }
                    return baos.toByteArray();
                }
            }

            @Override
            public MqttTopicSplit deserialize(int version, byte[] serialized) throws IOException {
                try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                     DataInputStream in = new DataInputStream(bais)) {
                    String topic = in.readUTF();
                    List<Integer> processedMessages = new LinkedList<>();
                    while (in.available() > 0) {
                        processedMessages.add(in.readInt());
                    }
                    return new MqttTopicSplit(topic, processedMessages);
                }
            }
        };
    }

    @Override
    public SimpleVersionedSerializer<MqttSourceEnumeratorState> getEnumeratorCheckpointSerializer() {
        return new SimpleVersionedSerializer<MqttSourceEnumeratorState>() {
            @Override
            public int getVersion() {
                return 0;
            }

            @Override
            public byte[] serialize(MqttSourceEnumeratorState enumeratorState) throws IOException {
                try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                     DataOutputStream out = new DataOutputStream(baos)) {
                    Set<String> assignedTopics = enumeratorState.getAssignedTopics();
                    out.writeInt(assignedTopics.size());
                    for (String topic : assignedTopics) {
                        out.writeUTF(topic);
                    }
                    Set<String> unassignedTopics = enumeratorState.getUnassignedTopics();
                    out.writeInt(unassignedTopics.size());
                    for (String topic : unassignedTopics) {
                        out.writeUTF(topic);
                    }
                    out.flush();

                    return baos.toByteArray();
                }
            }

            @Override
            public MqttSourceEnumeratorState deserialize(int version, byte[] serialized) throws IOException {
                try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                     DataInputStream in = new DataInputStream(bais)) {
                    HashSet<String> assignedTopics = new HashSet<>();
                    int numberOfAssignedTopics = in.readInt();
                    for (int i = 0; i < numberOfAssignedTopics; ++i) {
                        assignedTopics.add(in.readUTF());
                    }
                    HashSet<String> unassignedTopics = new HashSet<>();
                    int numberOfUnassignedTopics = in.readInt();
                    for (int i = 0; i < numberOfUnassignedTopics; ++i) {
                        unassignedTopics.add(in.readUTF());
                    }
                    return new MqttSourceEnumeratorState(assignedTopics, unassignedTopics);
                }
            }
        };
    }
}
