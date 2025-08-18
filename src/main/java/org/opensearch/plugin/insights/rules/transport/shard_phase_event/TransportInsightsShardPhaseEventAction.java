package org.opensearch.plugin.insights.rules.transport.shard_phase_event;

import java.io.IOException;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.plugin.insights.rules.action.shard_phase_event.InsightsShardPhaseEventAction;
import org.opensearch.plugin.insights.core.service.QueryInsightsService;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

public class TransportInsightsShardPhaseEventAction
    extends HandledTransportAction<ShardPhaseEventRequest, AcknowledgedResponse> {

    private final TransportService transportService;
    private final ClusterService clusterService;
    private final QueryInsightsService svc;

    @Inject
    public TransportInsightsShardPhaseEventAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        QueryInsightsService svc
    ) {
        // You can pass a specific executor; SAME is fine here (messages are tiny)
        super(InsightsShardPhaseEventAction.NAME, transportService, actionFilters,
            ShardPhaseEventRequest::new, ThreadPool.Names.SAME);
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.svc = svc;
    }

    @Override
    protected void doExecute(
        Task task,
        ShardPhaseEventRequest req,
        ActionListener<AcknowledgedResponse> listener
    ) {
        final String localNodeId = clusterService.localNode().getId();

        // If this isn't the coordinator, forward to the specified coordinator node
        if (!localNodeId.equals(req.coordinatorNodeId)) {
            final DiscoveryNode dest = clusterService.state().nodes().get(req.coordinatorNodeId);
            transportService.sendRequest(
                dest,
                InsightsShardPhaseEventAction.NAME,
                req,
                new TransportResponseHandler<AcknowledgedResponse>() {
                    @Override
                    public AcknowledgedResponse read(StreamInput in) throws IOException {
                        return new AcknowledgedResponse(in);
                    }

                    @Override
                    public void handleResponse(AcknowledgedResponse response) {
                        listener.onResponse(response);
                    }

                    @Override
                    public void handleException(TransportException e) {
                        listener.onFailure(e);
                    }

                    @Override
                    public String executor() {
                        return ThreadPool.Names.SAME;
                    }
                }
            );
            return;
        }

        // We are on the coordinator: record into the aggregator.
        switch (req.kind) {
            case START ->
                svc.recordShardPhaseStart(req.phase, req.shardId, req.dataNodeId, req.correlationId, req.wallMs);
            case END ->
                svc.recordShardPhaseEnd(req.phase, req.shardId, req.dataNodeId, req.correlationId, req.wallMs);
            case FAIL ->
                svc.recordShardPhaseFailure(req.phase, req.shardId, req.dataNodeId, req.correlationId, req.wallMs);
        }

        listener.onResponse(new AcknowledgedResponse(true));
    }
}
