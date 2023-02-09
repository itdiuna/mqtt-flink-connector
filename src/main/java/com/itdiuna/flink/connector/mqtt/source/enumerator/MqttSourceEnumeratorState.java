package com.itdiuna.flink.connector.mqtt.source.enumerator;

import java.util.Set;

public class MqttSourceEnumeratorState {

    private final Set<String> assignedTopics;

    private final Set<String> unassignedTopics;

    public MqttSourceEnumeratorState(Set<String> assignedTopics, Set<String> unassignedTopics) {
        this.assignedTopics = assignedTopics;
        this.unassignedTopics = unassignedTopics;
    }

    public Set<String> getAssignedTopics() {
        return assignedTopics;
    }

    public Set<String> getUnassignedTopics() {
        return unassignedTopics;
    }
}
