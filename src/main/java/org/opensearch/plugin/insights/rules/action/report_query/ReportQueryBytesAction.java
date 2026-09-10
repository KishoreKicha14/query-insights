/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.insights.rules.action.report_query;

/**
 * Shared constants for the "report query" transport channel that lets an external plugin (the SQL
 * plugin, for PPL/SQL queries) hand a completed query record to Query Insights so it flows through
 * the in-memory {@code addRecord} pipeline and appears in the <b>in-memory Top N</b> (and then the
 * historical index and ingest-time roll-up).
 * <p>
 * <b>Why a raw transport request handler and not an ActionType.</b> The request travels as the
 * <b>core</b> {@link org.opensearch.transport.BytesTransportRequest}. That type extends
 * {@code TransportRequest}, not {@code ActionRequest}, so it cannot be used with
 * {@code ActionType}/{@code client.execute}. Instead Query Insights registers a raw
 * {@link org.opensearch.transport.TransportRequestHandler} for this action name via
 * {@code TransportService.registerRequestHandler}, and the SQL plugin sends to it with
 * {@code transportService.sendRequest}. Using a core request type gives a single class identity
 * across both plugin classloaders, so same-node delivery does not hit a {@link ClassCastException}
 * the way a plugin-defined request type would. The response is the core
 * {@code TransportResponse.Empty.INSTANCE} for the same reason.
 * <p>
 * <b>Wire format</b> (carried inside the request bytes; versioned; mirrored on the SQL side — both
 * sides MUST agree):
 * <pre>
 *   vInt    FORMAT_VERSION (=1)
 *   String  querySource        (e.g. "PPL")
 *   String  coordinatorId      (parent marker: "&lt;source&gt;:&lt;nodeId&gt;:&lt;taskId&gt;")
 *   String  nodeId
 *   String  queryText
 *   vLong   timestampMillis
 *   vLong   latencyMillis
 *   vLong   cpuNanos
 *   vLong   memoryBytes
 *   vInt    phaseCount
 *   phaseCount x { String name, double timeMs, double cpuTimeMs, vLong memoryBytes }
 *   String  queryShapeHash     (SIMILARITY-grouping shape hash, may be empty)
 * </pre>
 * The SQL plugin and Query Insights are unreleased and always built together, so this is a single
 * format: any layout change is a coordinated change on both sides, not a compatibility boundary.
 */
public final class ReportQueryBytesAction {

    /**
     * The transport action name. The SQL plugin sends to this exact string; Query Insights
     * registers a request handler under it.
     */
    public static final String NAME = "cluster:admin/opensearch/query_insights/report_query_bytes";

    /** Wire format version. Must match {@code QueryInsightsReporter.FORMAT_VERSION} in the SQL plugin. */
    public static final int FORMAT_VERSION = 1;

    private ReportQueryBytesAction() {}
}
