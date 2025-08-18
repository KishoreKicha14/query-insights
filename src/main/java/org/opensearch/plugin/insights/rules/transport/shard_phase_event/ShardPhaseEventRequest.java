package org.opensearch.plugin.insights.rules.transport.shard_phase_event;

import org.opensearch.action.ActionRequest;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.action.ActionRequestValidationException;

import java.io.IOException;

public class ShardPhaseEventRequest extends ActionRequest {
    public enum Kind { START, END, FAIL }

    public String phase;               // "query" | "fetch"
    public ShardId shardId;
    public String dataNodeId;
    public long correlationId;         // parent (coordinator) task id
    public long wallMs;                // start or end wall-clock ms
    public Kind kind;
    public String coordinatorNodeId;   // where to aggregate

    public ShardPhaseEventRequest() {}
    public ShardPhaseEventRequest(StreamInput in) throws IOException {
        super(in);
        phase = in.readString();
        shardId = new ShardId(in);
        dataNodeId = in.readString();
        correlationId = in.readVLong();
        wallMs = in.readVLong();
        kind = in.readEnum(Kind.class);
        coordinatorNodeId = in.readString();
    }
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(phase);
        shardId.writeTo(out);
        out.writeString(dataNodeId);
        out.writeVLong(correlationId);
        out.writeVLong(wallMs);
        out.writeEnum(kind);
        out.writeString(coordinatorNodeId);
    }
    @Override
    public ActionRequestValidationException validate() {
        // add real checks if you want strict validation
        return null;
    }
}
