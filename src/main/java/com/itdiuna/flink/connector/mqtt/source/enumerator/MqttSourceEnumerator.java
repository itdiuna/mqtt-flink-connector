package com.itdiuna.flink.connector.mqtt.source.enumerator;

import com.itdiuna.flink.connector.mqtt.source.split.MqttTopicSplit;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class MqttSourceEnumerator implements SplitEnumerator<MqttTopicSplit, MqttSourceEnumeratorState> {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final Duration DISCOVERY_INTERVAL_MS = Duration.ofSeconds(5);

    private final SplitEnumeratorContext<MqttTopicSplit> context;

    private final Set<String> unassignedTopics;

    private final Set<String> assignedTopics;

    private final TopicStatusListener topicStatusListener;

    public MqttSourceEnumerator(SplitEnumeratorContext<MqttTopicSplit> context, String brokerURI, String topicFilter) {
        this(context, new HashSet<>(), new HashSet<>(), brokerURI, topicFilter);
    }

    public MqttSourceEnumerator(SplitEnumeratorContext<MqttTopicSplit> context, Set<String> assignedTopics, Set<String> unassignedTopics, String brokerURI, String topicFilter) {
        this.context = context;
        this.unassignedTopics = unassignedTopics;
        this.assignedTopics = assignedTopics;
        topicStatusListener = new TopicStatusListener(brokerURI, topicFilter);
    }

    @Override
    public void start() {
        topicStatusListener.start();
        context.callAsync(
                this::getUnassignedTopics,
                this::handleTopicChanges,
                0,
                DISCOVERY_INTERVAL_MS.toMillis()
        );
    }

    private TopicChanges getUnassignedTopics() {
        Set<String> topics = topicStatusListener.getDiscoveredTopics();
        topics.removeAll(assignedTopics);
        topics.addAll(unassignedTopics);
        return new TopicChanges(topics);
    }

    private void handleTopicChanges(TopicChanges topicChanges, Throwable throwable) {
        if (throwable != null) {
            throw new RuntimeException("Could not handle topic changes: " + topicChanges, throwable);
        }
        handleTopicChanges(topicChanges);
    }

    private void handleTopicChanges(TopicChanges topicChanges) {
        logger.info("New topics: {}", topicChanges.newTopics);

        for (String newTopic : topicChanges.newTopics) {
            context.assignSplit(new MqttTopicSplit(newTopic), resolveReader(newTopic));
            assignedTopics.add(newTopic);
            unassignedTopics.remove(newTopic);
        }
    }

    private int resolveReader(String topic) {
        return topic.hashCode() % context.currentParallelism();
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        logger.warn("reader {} requests for a new splits...", subtaskId);
    }

    @Override
    public void addSplitsBack(List<MqttTopicSplit> splits, int subtaskId) {
        logger.warn("reader {} returns splits: {}", subtaskId, splits);
        for (MqttTopicSplit split : splits) {
            unassignedTopics.add(split.splitId());
        }
    }

    @Override
    public void addReader(int subtaskId) {
        logger.warn("reader {} shows up", subtaskId);
    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        throw new RuntimeException("Source event type not handled: " + sourceEvent.getClass());
    }

    @Override
    public MqttSourceEnumeratorState snapshotState(long checkpointId) throws Exception {
        return new MqttSourceEnumeratorState(assignedTopics, unassignedTopics);
    }

    @Override
    public void close() throws IOException {
        logger.warn("closing - consider any resources to be closed");
    }

    static class TopicChanges {
        private final Set<String> newTopics;

        TopicChanges(Set<String> newTopics) {
            this.newTopics = newTopics;
        }
    }
}
