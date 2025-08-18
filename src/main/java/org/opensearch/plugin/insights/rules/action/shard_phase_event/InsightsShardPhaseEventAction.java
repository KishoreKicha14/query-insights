package org.opensearch.plugin.insights.rules.action.shard_phase_event;

import org.opensearch.action.ActionType;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.core.action.ActionListener;

public class InsightsShardPhaseEventAction {
    public static final String NAME = "internal:insights/shard_phase_event";
    public static final ActionType<AcknowledgedResponse> INSTANCE =
        new ActionType<>(NAME, AcknowledgedResponse::new);
    private InsightsShardPhaseEventAction() {}
}

