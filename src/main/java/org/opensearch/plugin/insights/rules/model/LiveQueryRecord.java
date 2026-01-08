/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.insights.rules.model;

/**
 * Lightweight record for live/finished queries
 */
public class LiveQueryRecord {
    private final String id;
    private final long timestamp;
    private final long latencyMs;
    private final long cpuNanos;
    private final long memoryBytes;
    private final String taskId;
    private final String nodeId;
    private final String description;
    private final boolean isCancelled;
    private final String wlmGroupId;
    private final String username;
    private final String[] userRoles;

    public LiveQueryRecord(
        String id,
        long timestamp,
        long latencyMs,
        long cpuNanos,
        long memoryBytes,
        String taskId,
        String nodeId,
        String description,
        boolean isCancelled,
        String wlmGroupId,
        String username,
        String[] userRoles
    ) {
        this.id = id;
        this.timestamp = timestamp;
        this.latencyMs = latencyMs;
        this.cpuNanos = cpuNanos;
        this.memoryBytes = memoryBytes;
        this.taskId = taskId;
        this.nodeId = nodeId;
        this.description = description;
        this.isCancelled = isCancelled;
        this.wlmGroupId = wlmGroupId;
        this.username = username;
        this.userRoles = userRoles;
    }

    public String getId() {
        return id;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public long getLatencyMs() {
        return latencyMs;
    }

    public long getCpuNanos() {
        return cpuNanos;
    }

    public long getMemoryBytes() {
        return memoryBytes;
    }

    public String getTaskId() {
        return taskId;
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getDescription() {
        return description;
    }

    public boolean isCancelled() {
        return isCancelled;
    }

    public String getWlmGroupId() {
        return wlmGroupId;
    }

    public String getUsername() {
        return username;
    }

    public String[] getUserRoles() {
        return userRoles;
    }
}
