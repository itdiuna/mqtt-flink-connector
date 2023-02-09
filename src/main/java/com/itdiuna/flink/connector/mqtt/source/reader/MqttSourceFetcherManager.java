package com.itdiuna.flink.connector.mqtt.source.reader;

import com.itdiuna.flink.connector.mqtt.source.split.MqttTopicSplit;
import com.itdiuna.flink.connector.mqtt.source.split.MqttTopicSplitReader;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcher;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.function.Supplier;

public class MqttSourceFetcherManager extends SingleThreadFetcherManager<MqttMessage, MqttTopicSplit> {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    public MqttSourceFetcherManager(FutureCompletingBlockingQueue<RecordsWithSplitIds<MqttMessage>> elementsQueue, Supplier<SplitReader<MqttMessage, MqttTopicSplit>> splitReaderSupplier) {
        super(elementsQueue, splitReaderSupplier);
    }

    public void sendAcknowledgements(List<Integer> processedMessages) {
        SplitFetcher<MqttMessage, MqttTopicSplit> fetcher = getRunningFetcher();
        if (fetcher != null) {
            MqttTopicSplitReader splitReader = (MqttTopicSplitReader) fetcher.getSplitReader();
            splitReader.acknowledge(processedMessages);
        } else {
            logger.warn("No fetcher is running to send acknowledgements");
        }
    }
}
