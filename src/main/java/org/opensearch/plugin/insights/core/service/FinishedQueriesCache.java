/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.insights.core.service;

import java.util.ArrayDeque;
import java.util.List;
import org.opensearch.plugin.insights.rules.model.LiveQueryRecord;

/**
 * Cache for recently finished queries
 */
public class FinishedQueriesCache {

    private static final int MAX_FINISHED_QUERIES = 1000;
    private static final int MAX_RETURNED_QUERIES = 50;
    private final long finishedQueryRetentionMs;
    private final long trackingInactivityMs;

    private final ArrayDeque<FinishedQuery> finishedQueries = new ArrayDeque<>();
    private volatile long lastAccessTime = 0;

    public FinishedQueriesCache(long finishedQueryRetentionMs, long trackingInactivityMs) {
        this.finishedQueryRetentionMs = finishedQueryRetentionMs;
        this.trackingInactivityMs = trackingInactivityMs;
    }

    private static class FinishedQuery {
        final LiveQueryRecord record;
        final long finishTime;

        FinishedQuery(LiveQueryRecord record, long finishTime) {
            this.record = record;
            this.finishTime = finishTime;
        }
    }

    public void addFinishedQuery(LiveQueryRecord record) {
        long currentTime = System.currentTimeMillis();
        if (lastAccessTime > 0 && currentTime - lastAccessTime > trackingInactivityMs) {
            clear();
            return;
        }
        lastAccessTime = currentTime;
        synchronized (finishedQueries) {
            // Clean up expired queries before adding new one
            while (!finishedQueries.isEmpty() && (currentTime - finishedQueries.peekFirst().finishTime) > finishedQueryRetentionMs) {
                finishedQueries.removeFirst();
            }
            finishedQueries.addLast(new FinishedQuery(record, currentTime));
            if (finishedQueries.size() > MAX_FINISHED_QUERIES) {
                finishedQueries.removeFirst();
            }
        }
    }

    public List<LiveQueryRecord> getFinishedQueries(boolean enableListener) {
        long currentTime = System.currentTimeMillis();
        if (lastAccessTime > 0 && currentTime - lastAccessTime > trackingInactivityMs) {
            clear();
            lastAccessTime = currentTime;
            return List.of();
        }
        lastAccessTime = currentTime;
        synchronized (finishedQueries) {
            while (!finishedQueries.isEmpty() && (lastAccessTime - finishedQueries.peekFirst().finishTime) > finishedQueryRetentionMs) {
                finishedQueries.removeFirst();
            }
            return finishedQueries.stream()
                .sorted((a, b) -> Long.compare(b.finishTime, a.finishTime))
                .limit(MAX_RETURNED_QUERIES)
                .map(fq -> fq.record)
                .toList();
        }
    }

    public void clear() {
        synchronized (finishedQueries) {
            finishedQueries.clear();
        }
    }
}
