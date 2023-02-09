package com.itdiuna.flink.connector.mqtt.source.reader;

import com.itdiuna.flink.connector.mqtt.source.split.MqttTopicSplit;
import com.itdiuna.flink.connector.mqtt.source.split.MqttTopicSplitReader;
import com.itdiuna.flink.connector.mqtt.source.split.MqttTopicSplitState;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Function;

public class MqttSourceReader<T> extends SingleThreadMultiplexSourceReaderBase<MqttMessage, T, MqttTopicSplit, MqttTopicSplitState> {

    private final Map<Long, List<Integer>> processedMessages = new ConcurrentSkipListMap<>();

    public MqttSourceReader(SourceReaderContext context, FutureCompletingBlockingQueue<RecordsWithSplitIds<MqttMessage>> messagesQueue, Function<MqttMessage, T> mapper, Function<T, Long> timestampExtractor, String brokerURI) {
        super(
                messagesQueue,
                new MqttSourceFetcherManager(messagesQueue, () -> new MqttTopicSplitReader(context, brokerURI)),
                new RecordEmitter<MqttMessage, T, MqttTopicSplitState>() {
                    final Logger logger = LoggerFactory.getLogger(MqttSourceReader.class);

                    @Override
                    public void emitRecord(MqttMessage element, SourceOutput<T> output, MqttTopicSplitState splitState) throws Exception {
                        T record = mapper.apply(element);
                        output.collect(record, timestampExtractor.apply(record));
                        splitState.process(element.getId());
                        logger.info("Message has been emitted: {}", element.getId());
                    }
                },
                new Configuration(),
                context
        );
    }

    @Override
    public List<MqttTopicSplit> snapshotState(long checkpointId) {
        processedMessages.put(checkpointId, new LinkedList<>());
        List<MqttTopicSplit> topicSplits = super.snapshotState(checkpointId);
        for (MqttTopicSplit topicSplit : topicSplits) {
            processedMessages.get(checkpointId).addAll(topicSplit.getProcessedMessages());
            topicSplit.clearProcessedMessages();
        }
        return topicSplits;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        ((MqttSourceFetcherManager) splitFetcherManager).sendAcknowledgements(processedMessages.remove(checkpointId));
    }

    @Override
    protected void onSplitFinished(Map<String, MqttTopicSplitState> finishedSplitIds) {

    }

    @Override
    protected MqttTopicSplitState initializedState(MqttTopicSplit split) {
        return new MqttTopicSplitState();
    }

    @Override
    protected MqttTopicSplit toSplitType(String splitId, MqttTopicSplitState splitState) {
        return new MqttTopicSplit(splitId, splitState.streamProcessedMessages());
    }
}
