/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.insights.rules.transport.report_query;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.plugin.insights.core.service.QueryInsightsService;
import org.opensearch.plugin.insights.rules.action.report_query.ReportQueryBytesAction;
import org.opensearch.plugin.insights.rules.model.AggregationType;
import org.opensearch.plugin.insights.rules.model.Attribute;
import org.opensearch.plugin.insights.rules.model.Measurement;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.plugin.insights.rules.model.SourceString;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;
import org.opensearch.tasks.Task;
import org.opensearch.transport.BytesTransportRequest;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportRequestHandler;

/**
 * Raw transport request handler for the "report query" channel (see {@link ReportQueryBytesAction}).
 * <p>
 * Registered under {@link ReportQueryBytesAction#NAME} against the core
 * {@link BytesTransportRequest}. Deserializes the versioned PPL/SQL record payload the SQL plugin
 * serialized on the coordinator, builds a {@link SearchQueryRecord} shaped identically to what the
 * native search listener produces for a top-level query, and feeds it into
 * {@link QueryInsightsService#addRecord(SearchQueryRecord)} so it flows through the same in-memory
 * pipeline as DSL searches — landing in the in-memory Top N, the historical index, and the
 * ingest-time roll-up.
 * <p>
 * Any deserialization or ingestion failure is logged and swallowed; the caller always gets an ack
 * ({@link TransportResponse.Empty#INSTANCE}) so a reporting hiccup never fails the originating
 * query.
 */
public class ReportQueryBytesRequestHandler implements TransportRequestHandler<BytesTransportRequest> {

    private static final Logger logger = LogManager.getLogger(ReportQueryBytesRequestHandler.class);

    private final QueryInsightsService queryInsightsService;

    /**
     * Constructor.
     *
     * @param queryInsightsService the service that owns the in-memory Top N pipeline
     */
    public ReportQueryBytesRequestHandler(final QueryInsightsService queryInsightsService) {
        this.queryInsightsService = queryInsightsService;
    }

    @Override
    public void messageReceived(final BytesTransportRequest request, final TransportChannel channel, final Task task) throws Exception {
        try {
            final SearchQueryRecord record = deserialize(request);
            if (record != null) {
                queryInsightsService.addRecord(record);
            }
        } catch (final Exception e) {
            logger.warn("Failed to ingest reported PPL/SQL query record into Query Insights", e);
        }
        // Always ack with a core response type so the same-node response path is classloader-safe.
        channel.sendResponse(TransportResponse.Empty.INSTANCE);
    }

    /**
     * Deserialize the versioned payload carried in the request bytes into a {@link SearchQueryRecord}.
     *
     * @param request the bytes request
     * @return the record, or {@code null} if the payload version is unrecognized
     * @throws java.io.IOException on malformed payload
     */
    private SearchQueryRecord deserialize(final BytesTransportRequest request) throws java.io.IOException {
        try (StreamInput in = request.bytes().streamInput()) {
            final int version = in.readVInt();
            if (version != ReportQueryBytesAction.FORMAT_VERSION) {
                logger.warn("Ignoring reported query record with unsupported wire version {}", version);
                return null;
            }
            final String querySource = in.readString();
            final String coordinatorId = in.readString();
            final String nodeId = in.readString();
            final String queryText = in.readString();
            final long timestampMillis = in.readVLong();
            final long latencyMillis = in.readVLong();
            final long cpuNanos = in.readVLong();
            final long memoryBytes = in.readVLong();

            final int phaseCount = in.readVInt();
            final Map<String, Map<String, Object>> phases = new LinkedHashMap<>();
            for (int i = 0; i < phaseCount; i++) {
                final String name = in.readString();
                final double timeMs = in.readDouble();
                final double cpuTimeMs = in.readDouble();
                final long phaseMemBytes = in.readVLong();
                final Map<String, Object> p = new HashMap<>();
                p.put("time_ms", timeMs);
                p.put("cpu_time_ms", cpuTimeMs);
                p.put("memory_bytes", phaseMemBytes);
                phases.put(name, p);
            }

            // The shape hash carried in the payload for future SIMILARITY grouping of PPL/SQL
            // queries. Read to keep the wire format aligned with the SQL plugin, but intentionally
            // NOT applied for now: PPL/SQL similarity grouping is deferred (see the
            // feat/ppl-query-insights-grouping branch). Because QUERY_GROUP_HASHCODE is never set on
            // these records, they always fall through as ungrouped even when group_by=SIMILARITY.
            @SuppressWarnings("unused")
            final String queryShapeHash = in.readString();

            final Map<MetricType, Measurement> measurements = new HashMap<>();
            measurements.put(MetricType.LATENCY, new Measurement(latencyMillis, AggregationType.NONE));
            measurements.put(MetricType.CPU, new Measurement(cpuNanos, AggregationType.NONE));
            measurements.put(MetricType.MEMORY, new Measurement(memoryBytes, AggregationType.NONE));

            final Map<Attribute, Object> attributes = new HashMap<>();
            attributes.put(Attribute.QUERY_SOURCE, querySource);
            attributes.put(Attribute.NODE_ID, nodeId);
            // Store the (prefix-stripped) query text as the record's SOURCE so the details view can
            // display the originating PPL/SQL query. Setting it here also prevents the drain-time
            // setSourceAndTruncation from overwriting it with an empty string (it only fills SOURCE
            // when absent). A PPL/SQL query has no DSL SearchSourceBuilder, so this is the only
            // place the query text can come from.
            if (queryText != null && queryText.isEmpty() == false) {
                attributes.put(Attribute.SOURCE, new SourceString(queryText));
            }
            // Shape this record like a native (ungrouped) query so downstream ranking, the Top N
            // overview, and the dashboard treat it as a query rather than a similarity group. The
            // native search listener sets this explicitly; without it group_by is absent and
            // consumers that check group_by == "NONE" (e.g. the dashboard's timestamp/status
            // columns) misclassify the record as "Aggregated".
            attributes.put(Attribute.GROUP_BY, QueryInsightsSettings.DEFAULT_GROUPING_TYPE);
            // Top-level (parent) query. Its own marker is the coordinatorId; child DSL sub-query
            // records reference it via DERIVED_FROM so their CPU/memory roll up into this record.
            attributes.put(Attribute.PARENT_MARKER, coordinatorId);
            if (phases.isEmpty() == false) {
                attributes.put(Attribute.PHASES, phases);
            }
            // NOTE: QUERY_GROUP_HASHCODE is intentionally not set here. PPL/SQL similarity grouping
            // is deferred; without this attribute the grouper leaves these records ungrouped even
            // under group_by=SIMILARITY. The grouping implementation is preserved on the
            // feat/ppl-query-insights-grouping branch for future pickup.

            // Record id: give the parent SQL/PPL record a DSL-style random UUID rather than reusing
            // the coordinatorId ("<source>:<nodeId>:<taskId>"). The colon-delimited, source-prefixed
            // coordinatorId is retained only as PARENT_MARKER (above), which is what child DSL
            // sub-queries reference via DERIVED_FROM and what the roll-up / sub-query detail lookup
            // matches on — none of that keys off the record id. Passing null lets the
            // SearchQueryRecord constructor generate UUID.randomUUID().toString(), identical to the
            // native search listener's DSL records, so a parent's id is indistinguishable in format
            // from a DSL query's id (no "PPL:"/"SQL:" prefix leaking into the id).
            return new SearchQueryRecord(timestampMillis, measurements, attributes, null);
        }
    }
}
