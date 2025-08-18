package org.opensearch.plugin.insights.core.listener;

import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.shard.SearchOperationListener;
import org.opensearch.plugin.insights.core.service.QueryInsightsService;
import org.opensearch.plugin.insights.rules.transport.shard_phase_event.ShardPhaseEventRequest;
import org.opensearch.search.internal.SearchContext;

public final class ShardTapListener implements SearchOperationListener {
    private final QueryInsightsService svc;
    private final ClusterService clusterService;

    public ShardTapListener(QueryInsightsService svc, ClusterService clusterService) {
        this.svc = svc;
        this.clusterService = clusterService;
    }

    @Override public void onPreQueryPhase(SearchContext ctx)  { send(ctx, "query", ShardPhaseEventRequest.Kind.START); }
    @Override public void onQueryPhase(SearchContext ctx, long tookNanos) { send(ctx, "query", ShardPhaseEventRequest.Kind.END); }
    @Override public void onFailedQueryPhase(SearchContext ctx) { send(ctx, "query", ShardPhaseEventRequest.Kind.FAIL); }

    @Override public void onPreFetchPhase(SearchContext ctx)  { send(ctx, "fetch", ShardPhaseEventRequest.Kind.START); }
    @Override public void onFetchPhase(SearchContext ctx, long tookNanos) { send(ctx, "fetch", ShardPhaseEventRequest.Kind.END); }
    @Override public void onFailedFetchPhase(SearchContext ctx) { send(ctx, "fetch", ShardPhaseEventRequest.Kind.FAIL); }

    private void send(SearchContext ctx, String phase, ShardPhaseEventRequest.Kind kind) {
        var t = ctx.shardTarget(); if (t == null) return;
        var task = ctx.getTask();

        long corrId = task.getParentTaskId().isSet()
            ? task.getParentTaskId().getId()
            : task.getId();

        // If no parent, treat this *local* node as the coordinator
        String coordNode = task.getParentTaskId().isSet()
            ? task.getParentTaskId().getNodeId()
            : clusterService.localNode().getId();

        ShardPhaseEventRequest req = new ShardPhaseEventRequest();
        req.phase = phase;
        req.kind = kind;
        req.shardId = t.getShardId();
        req.dataNodeId = t.getNodeId();
        req.correlationId = corrId;
        req.wallMs = System.currentTimeMillis();
        req.coordinatorNodeId = coordNode;

        svc.sendShardPhaseEvent(req, new ActionListener<AcknowledgedResponse>() {
            @Override public void onResponse(AcknowledgedResponse resp) { /* noop */ }
            @Override public void onFailure(Exception e) { /* optional log */ }
        });
    }
}
