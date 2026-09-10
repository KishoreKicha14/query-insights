/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.transport.top_queries;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.nodes.TransportNodesAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.plugin.insights.core.auth.TopQueriesRbacFilter;
import org.opensearch.plugin.insights.core.auth.UserPrincipalContext;
import org.opensearch.plugin.insights.core.auth.UserPrincipalContext.UserPrincipalInfo;
import org.opensearch.plugin.insights.core.service.QueryInsightsService;
import org.opensearch.plugin.insights.core.service.recommendations.RecommendationService;
import org.opensearch.plugin.insights.rules.action.top_queries.TopQueries;
import org.opensearch.plugin.insights.rules.action.top_queries.TopQueriesAction;
import org.opensearch.plugin.insights.rules.action.top_queries.TopQueriesRequest;
import org.opensearch.plugin.insights.rules.action.top_queries.TopQueriesResponse;
import org.opensearch.plugin.insights.rules.model.Attribute;
import org.opensearch.plugin.insights.rules.model.Measurement;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.rules.model.FilterByMode;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.plugin.insights.rules.model.recommendations.Recommendation;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportService;

/**
 * Transport action for cluster/node level top queries information.
 */
public class TransportTopQueriesAction extends TransportNodesAction<
    TopQueriesRequest,
    TopQueriesResponse,
    TransportTopQueriesAction.NodeRequest,
    TopQueries> {

    private static final Logger log = LogManager.getLogger(TransportTopQueriesAction.class);

    private final QueryInsightsService queryInsightsService;

    /**
     * Create the TransportTopQueriesAction Object
     *
     * @param threadPool The OpenSearch thread pool to run async tasks
     * @param clusterService The clusterService of this node
     * @param transportService The TransportService of this node
     * @param queryInsightsService The queryInsightsService associated with this Transport Action
     * @param actionFilters the action filters
     */
    @Inject
    public TransportTopQueriesAction(
        final ThreadPool threadPool,
        final ClusterService clusterService,
        final TransportService transportService,
        final QueryInsightsService queryInsightsService,
        final ActionFilters actionFilters
    ) {
        super(
            TopQueriesAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            TopQueriesRequest::new,
            NodeRequest::new,
            ThreadPool.Names.GENERIC,
            TopQueries.class
        );
        this.queryInsightsService = queryInsightsService;
    }

    ActionListener<TopQueriesResponse> createInMemoryDataCollectionListener(
        TopQueriesRequest request,
        FilterByMode filterByMode,
        UserPrincipalInfo userInfo,
        ActionListener<TopQueriesResponse> finalListener
    ) {
        return new ActionListener<TopQueriesResponse>() {
            @Override
            public void onResponse(TopQueriesResponse inMemoryQueriesResponse) {
                handleInMemoryDataResponse(request, filterByMode, userInfo, inMemoryQueriesResponse, finalListener);
            }

            @Override
            public void onFailure(Exception e) {
                finalListener.onFailure(e);
            }
        };
    }

    void handleInMemoryDataResponse(
        TopQueriesRequest request,
        FilterByMode filterByMode,
        UserPrincipalInfo userInfo,
        TopQueriesResponse inMemoryQueriesResponse,
        ActionListener<TopQueriesResponse> finalListener
    ) {
        List<TopQueries> inMemoryTopQueries = inMemoryQueriesResponse.getNodes();
        List<FailedNodeException> inMemoryDataFailures = inMemoryQueriesResponse.failures();
        String from = request.getFrom();
        String to = request.getTo();
        if (from != null && to != null) {
            fetchHistoricalData(request, filterByMode, userInfo, inMemoryTopQueries, inMemoryDataFailures, finalListener);
        } else {
            finalListener.onResponse(
                new TopQueriesResponse(clusterService.getClusterName(), inMemoryTopQueries, inMemoryDataFailures, request.getMetricType())
            );
        }
    }

    void fetchHistoricalData(
        TopQueriesRequest request,
        FilterByMode filterByMode,
        UserPrincipalInfo userInfo,
        List<TopQueries> inMemoryTopQueries,
        List<FailedNodeException> inMemoryDataFailures,
        ActionListener<TopQueriesResponse> finalListener
    ) {
        String from = request.getFrom();
        String to = request.getTo();
        String id = request.getId();
        Boolean verbose = request.getVerbose();

        // Resolve RBAC filter values for the index query
        String username = null;
        List<String> backendRoles = null;
        if (filterByMode != FilterByMode.NONE && userInfo != null && !TopQueriesRbacFilter.isAdmin(userInfo)) {
            switch (filterByMode) {
                case USERNAME:
                    username = userInfo.getUserName();
                    break;
                case BACKEND_ROLES:
                    backendRoles = userInfo.getBackendRoles();
                    break;
                default:
                    break;
            }
        }

        try (final ThreadContext.StoredContext storedContext = threadPool.getThreadContext().stashContext()) {
            queryInsightsService.getTopQueriesService(request.getMetricType())
                .getTopQueriesRecordsFromIndex(
                    from,
                    to,
                    id,
                    verbose,
                    username,
                    backendRoles,
                    new ActionListener<List<SearchQueryRecord>>() {
                        @Override
                        public void onResponse(List<SearchQueryRecord> historicalRecords) {
                            onHistoricalDataResponse(request, inMemoryTopQueries, inMemoryDataFailures, historicalRecords, finalListener);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            onHistoricalDataFailure(request, inMemoryTopQueries, inMemoryDataFailures, e, finalListener);
                        }
                    }
                );
        } catch (Exception e) {
            logger.error("Synchronous failure while initiating historical top queries fetch", e);
            finalListener.onFailure(e);
        }
    }

    /**
     * Roll up the CPU and memory of child DSL sub-queries into their SQL/PPL parent so a parent's
     * reported cost reflects its own engine cost plus all its DSL searches.
     *
     * <p>Algorithm: first group the child records ({@code is_child == true}) by {@code derived_from}
     * (the parent marker), summing CPU and memory per group; then, for each parent record (carrying
     * a {@code parent_marker}), add the matching group's CPU/memory into the parent's measurements.
     * Latency is NOT summed — the parent already carries its own end-to-end wall-clock latency.
     *
     * <p>The passed list must contain BOTH the parents and their children (the historical read
     * returns both). Child records are left unchanged (their individual costs remain for the detail
     * view); only parent measurements are augmented. Runs at read time, so it works with records that
     * were written directly to the index (i.e. that never flowed through the in-memory ingest).
     *
     * @param records records fetched for the current window (parents + children)
     */
    static void rollUpChildrenIntoParents(final List<SearchQueryRecord> records) {
        if (records == null || records.isEmpty()) {
            return;
        }
        // 1) Group children by parent marker, summing cpu + memory.
        final Map<String, long[]> childTotals = new HashMap<>();
        for (SearchQueryRecord record : records) {
            if (!Boolean.TRUE.equals(record.getAttributes().get(Attribute.IS_CHILD))) {
                continue;
            }
            final Object derivedFrom = record.getAttributes().get(Attribute.DERIVED_FROM);
            if (!(derivedFrom instanceof String) || ((String) derivedFrom).isEmpty()) {
                continue;
            }
            final long[] acc = childTotals.computeIfAbsent((String) derivedFrom, k -> new long[2]);
            acc[0] += asLong(record.getMeasurement(MetricType.CPU));
            acc[1] += asLong(record.getMeasurement(MetricType.MEMORY));
        }
        if (childTotals.isEmpty()) {
            return;
        }
        // 2) Add each group's totals to the parent whose marker matches.
        for (SearchQueryRecord record : records) {
            if (Boolean.TRUE.equals(record.getAttributes().get(Attribute.IS_CHILD))) {
                continue;
            }
            final Object marker = record.getAttributes().get(Attribute.PARENT_MARKER);
            if (!(marker instanceof String)) {
                continue;
            }
            final long[] acc = childTotals.get((String) marker);
            if (acc == null) {
                continue;
            }
            // Set the parent's measurement to (own + Σchildren). We compute and set the total
            // explicitly rather than calling addMeasurement, because these records use aggregation
            // type NONE, for which addMeasurement REPLACES the value instead of summing it.
            setTotal(record, MetricType.CPU, acc[0]);
            setTotal(record, MetricType.MEMORY, acc[1]);
        }
    }

    /**
     * Set {@code record}'s measurement for {@code metricType} to its current value plus {@code add}.
     * Handles the aggregation-type-NONE case (where {@link SearchQueryRecord#addMeasurement} would
     * replace rather than add) by reading the current value and writing the sum directly.
     */
    private static void setTotal(final SearchQueryRecord record, final MetricType metricType, final long add) {
        if (add == 0L) {
            return;
        }
        final long own = asLong(record.getMeasurement(metricType));
        final Measurement existing = record.getMeasurements().get(metricType);
        if (existing != null) {
            existing.setMeasurement(own + add);
        } else {
            record.getMeasurements().put(metricType, new Measurement(own + add));
        }
    }

    private static long asLong(final Number number) {
        return number == null ? 0L : number.longValue();
    }

    void onHistoricalDataResponse(
        TopQueriesRequest request,
        List<TopQueries> inMemoryTopQueries,
        List<FailedNodeException> inMemoryDataFailures,
        List<SearchQueryRecord> historicalRecords,
        ActionListener<TopQueriesResponse> finalListener
    ) {
        List<TopQueries> combinedTopQueriesList = new ArrayList<>(inMemoryTopQueries);
        if (historicalRecords != null && !historicalRecords.isEmpty()) {
            // Remove duplicates between in-memory and historical records
            List<SearchQueryRecord> deduplicatedHistoricalRecords = removeDuplicates(inMemoryTopQueries, historicalRecords);
            // Roll child DSL cpu/memory up into their SQL/PPL parents BEFORE excluding children and
            // ranking, so the overview ranks parents by their true total cost (own + Σchildren).
            // Must run while children are still present in the list.
            //
            // Only for the overview (id == null). For the detail view (id != null) the roll-up is
            // done once in appendChildrenAndRespond against the authoritative child fetch; rolling up
            // here as well would double-count the children into the parent.
            if (request.getId() == null) {
                rollUpChildrenIntoParents(deduplicatedHistoricalRecords);
            }
            // Exclude child DSL sub-queries of SQL/PPL queries from the Top N overview, mirroring the
            // in-memory nodeOperation path. Children still live in the index and are surfaced under
            // the parent query's detail view. When a specific record id is requested (detail view),
            // do not filter — the caller wants that exact record (and, below, its children).
            if (request.getId() == null && !deduplicatedHistoricalRecords.isEmpty()) {
                deduplicatedHistoricalRecords = deduplicatedHistoricalRecords.stream()
                    .filter(record -> !Boolean.TRUE.equals(record.getAttributes().get(Attribute.IS_CHILD)))
                    // Re-rank by the requested metric after the roll-up so the ordering reflects the
                    // rolled-up total (the reader's original sort was on pre-roll-up values).
                    .sorted((a, b) -> SearchQueryRecord.compare(a, b, request.getMetricType()) * -1)
                    .collect(Collectors.toList());
            }
            if (!deduplicatedHistoricalRecords.isEmpty()) {
                // Pre-compute recommendations for historical records (in-memory records already have them from nodeOperation)
                Map<String, List<Recommendation>> historicalRecs = Collections.emptyMap();
                if (Boolean.TRUE.equals(request.getRecommendations())) {
                    historicalRecs = new HashMap<>();
                    RecommendationService recommendationService = queryInsightsService.getRecommendationService();
                    for (SearchQueryRecord record : deduplicatedHistoricalRecords) {
                        List<Recommendation> recs = (recommendationService != null)
                            ? recommendationService.generateRecommendations(record)
                            : null;
                        historicalRecs.put(record.getId(), recs != null ? recs : Collections.emptyList());
                    }
                }
                combinedTopQueriesList.add(new TopQueries(clusterService.localNode(), deduplicatedHistoricalRecords, historicalRecs));
            }
        }

        // Detail view: when a specific parent id is requested, additionally fetch that parent's child
        // sub-queries (records whose derived_from == the parent's marker) and append them so the
        // detail page can render sub-queries. This is additive — the parent record and its
        // recommendations are already assembled above.
        if (request.getId() != null) {
            appendChildrenAndRespond(request, combinedTopQueriesList, inMemoryDataFailures, finalListener);
            return;
        }

        finalListener.onResponse(
            new TopQueriesResponse(clusterService.getClusterName(), combinedTopQueriesList, inMemoryDataFailures, request.getMetricType())
        );
    }

    /**
     * Detail-view path (a specific parent {@code id} was requested). Given the already-assembled
     * parent record list, fetches the parent's child sub-queries (records whose {@code derived_from}
     * equals the parent's {@code parent_marker}) and appends them so the detail view can render
     * sub-queries. Best-effort: if the parent has no marker or the child fetch fails, responds with
     * the parent record(s) alone.
     */
    void appendChildrenAndRespond(
        TopQueriesRequest request,
        List<TopQueries> combined,
        List<FailedNodeException> inMemoryDataFailures,
        ActionListener<TopQueriesResponse> finalListener
    ) {
        // Resolve the requested parent's marker and hold a reference to the parent record so its
        // cpu/memory can be augmented with the rolled-up child totals once the children are fetched.
        String parentMarker = null;
        SearchQueryRecord parentRecord = null;
        for (TopQueries tq : combined) {
            if (tq.getTopQueriesRecord() == null) {
                continue;
            }
            for (SearchQueryRecord record : tq.getTopQueriesRecord()) {
                if (request.getId().equals(record.getId())) {
                    parentRecord = record;
                    Object marker = record.getAttributes().get(Attribute.PARENT_MARKER);
                    if (marker instanceof String) {
                        parentMarker = (String) marker;
                    }
                }
            }
        }
        final SearchQueryRecord parent = parentRecord;

        // No marker (plain DSL query or no parent linkage) → nothing to expand; respond as-is.
        if (parentMarker == null || parentMarker.isEmpty() || request.getFrom() == null || request.getTo() == null) {
            finalListener.onResponse(
                new TopQueriesResponse(clusterService.getClusterName(), combined, inMemoryDataFailures, request.getMetricType())
            );
            return;
        }

        queryInsightsService.getTopQueriesService(request.getMetricType())
            .getChildrenFromIndex(
                request.getFrom(),
                request.getTo(),
                parentMarker,
                request.getVerbose(),
                new ActionListener<List<SearchQueryRecord>>() {
                    @Override
                    public void onResponse(List<SearchQueryRecord> children) {
                        if (children != null && !children.isEmpty()) {
                            // Fold the children's cpu/memory into the parent so the detail view shows
                            // the rolled-up total, then append the children for the sub-query list.
                            if (parent != null) {
                                List<SearchQueryRecord> parentAndChildren = new ArrayList<>(children.size() + 1);
                                parentAndChildren.add(parent);
                                parentAndChildren.addAll(children);
                                rollUpChildrenIntoParents(parentAndChildren);
                            }
                            combined.add(new TopQueries(clusterService.localNode(), children, Collections.emptyMap()));
                        }
                        finalListener.onResponse(
                            new TopQueriesResponse(
                                clusterService.getClusterName(),
                                combined,
                                inMemoryDataFailures,
                                request.getMetricType()
                            )
                        );
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.warn("Failed to fetch child sub-queries for detail view; returning parent only.", e);
                        finalListener.onResponse(
                            new TopQueriesResponse(
                                clusterService.getClusterName(),
                                combined,
                                inMemoryDataFailures,
                                request.getMetricType()
                            )
                        );
                    }
                }
            );
    }

    void onHistoricalDataFailure(
        TopQueriesRequest request,
        List<TopQueries> inMemoryTopQueries,
        List<FailedNodeException> inMemoryDataFailures,
        Exception e,
        ActionListener<TopQueriesResponse> finalListener
    ) {
        logger.warn("Failed to fetch historical top queries, proceeding with in-memory data only.", e);
        finalListener.onResponse(
            new TopQueriesResponse(clusterService.getClusterName(), inMemoryTopQueries, inMemoryDataFailures, request.getMetricType())
        );
    }

    /**
     * Remove duplicate records from historical data that already exist in in-memory data.
     * Uses a Set to maintain unique record IDs for efficient lookup and comparison.
     *
     * @param inMemoryTopQueries List of TopQueries containing in-memory records
     * @param historicalRecords List of historical SearchQueryRecord objects
     * @return List of deduplicated historical records
     */
    private List<SearchQueryRecord> removeDuplicates(List<TopQueries> inMemoryTopQueries, List<SearchQueryRecord> historicalRecords) {
        // Collect all in-memory record IDs into a set for efficient lookup
        Set<String> inMemoryRecordIds = new LinkedHashSet<>();
        for (TopQueries topQueries : inMemoryTopQueries) {
            if (topQueries.getTopQueriesRecord() != null) {
                for (SearchQueryRecord record : topQueries.getTopQueriesRecord()) {
                    inMemoryRecordIds.add(record.getId());
                }
            }
        }

        // Filter out historical records that have IDs already present in in-memory data
        List<SearchQueryRecord> deduplicatedRecords = new ArrayList<>();
        for (SearchQueryRecord historicalRecord : historicalRecords) {
            if (!inMemoryRecordIds.contains(historicalRecord.getId())) {
                deduplicatedRecords.add(historicalRecord);
            }
        }

        return deduplicatedRecords;
    }

    @Override
    protected void doExecute(Task task, TopQueriesRequest request, ActionListener<TopQueriesResponse> finalListener) {
        // Capture filter mode and user info before super.doExecute() which may stash context
        FilterByMode filterByMode = queryInsightsService.getFilterByMode();
        UserPrincipalInfo userInfo = null;

        if (filterByMode != FilterByMode.NONE) {
            try {
                userInfo = new UserPrincipalContext(threadPool).extractUserInfo();
            } catch (Exception e) {
                log.warn("Failed to extract user info for RBAC filtering", e);
            }
            if (userInfo == null) {
                log.warn("User info unavailable with filter_by_mode [{}], denying access", filterByMode);
                finalListener.onResponse(
                    new TopQueriesResponse(
                        clusterService.getClusterName(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        request.getMetricType()
                    )
                );
                return;
            }
        }

        ActionListener<TopQueriesResponse> rbacListener = wrapWithRbacFilter(finalListener, filterByMode, userInfo);
        super.doExecute(task, request, createInMemoryDataCollectionListener(request, filterByMode, userInfo, rbacListener));
    }

    /**
     * Wraps the final listener with an RBAC filtering step that filters records in each node's response.
     */
    ActionListener<TopQueriesResponse> wrapWithRbacFilter(
        ActionListener<TopQueriesResponse> delegate,
        FilterByMode filterByMode,
        UserPrincipalInfo userInfo
    ) {
        if (filterByMode == FilterByMode.NONE) {
            return delegate;
        }
        final FilterByMode mode = filterByMode;
        final UserPrincipalInfo info = userInfo;
        return new ActionListener<TopQueriesResponse>() {
            @Override
            public void onResponse(TopQueriesResponse response) {
                try {
                    List<TopQueries> filteredNodes = response.getNodes().stream().map(topQueries -> {
                        List<SearchQueryRecord> filtered = TopQueriesRbacFilter.filterRecords(topQueries.getTopQueriesRecord(), mode, info);
                        Set<String> filteredIds = filtered.stream().map(SearchQueryRecord::getId).collect(Collectors.toSet());
                        Map<String, List<Recommendation>> filteredRecs = topQueries.getRecommendations()
                            .entrySet()
                            .stream()
                            .filter(e -> filteredIds.contains(e.getKey()))
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                        return new TopQueries(topQueries.getNode(), filtered, filteredRecs);
                    }).collect(Collectors.toList());
                    delegate.onResponse(
                        new TopQueriesResponse(response.getClusterName(), filteredNodes, response.failures(), response.getMetricType())
                    );
                } catch (Exception e) {
                    delegate.onFailure(e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                delegate.onFailure(e);
            }
        };
    }

    @Override
    protected TopQueriesResponse newResponse(
        final TopQueriesRequest topQueriesRequest,
        final List<TopQueries> collectedNodeResponses,
        final List<FailedNodeException> failures
    ) {
        return new TopQueriesResponse(clusterService.getClusterName(), collectedNodeResponses, failures, topQueriesRequest.getMetricType());
    }

    @Override
    protected NodeRequest newNodeRequest(final TopQueriesRequest request) {
        return new NodeRequest(request);
    }

    @Override
    protected TopQueries newNodeResponse(final StreamInput in) throws IOException {
        return new TopQueries(in);
    }

    @Override
    protected TopQueries nodeOperation(final NodeRequest nodeRequest) {
        final TopQueriesRequest request = nodeRequest.request;
        List<SearchQueryRecord> allRecords = queryInsightsService.getTopQueriesService(request.getMetricType())
            .getTopQueriesRecords(true, request.getFrom(), request.getTo(), request.getId(), request.getVerbose());

        // Roll child DSL cpu/memory up into their SQL/PPL parents while children are still present,
        // so the in-memory overview reflects the parent's true total cost (own + Σchildren). This
        // mirrors the historical path (onHistoricalDataResponse). For PPL via the report-query
        // channel, both parent and children flow through addRecord, so both are present in this
        // in-memory window.
        //
        // Only for the overview (id == null). For the detail view (id != null) the roll-up is done
        // once in appendChildrenAndRespond against the authoritative child fetch; rolling up here as
        // well would double-count the children into the parent.
        if (request.getId() == null) {
            rollUpChildrenIntoParents(allRecords);
        }

        // Exclude child DSL sub-queries of SQL/PPL queries from the Top N overview. They remain in
        // the store and are surfaced under the parent query's detail view (sub-queries). When a
        // specific record id is requested (detail view), do not filter.
        List<SearchQueryRecord> records;
        if (request.getId() != null) {
            records = allRecords;
        } else {
            records = new ArrayList<>(allRecords.size());
            for (SearchQueryRecord record : allRecords) {
                if (!Boolean.TRUE.equals(record.getAttributes().get(Attribute.IS_CHILD))) {
                    records.add(record);
                }
            }
            // Re-rank by the requested metric after the roll-up so the ordering reflects the
            // rolled-up total rather than the parents' pre-roll-up own cost.
            records.sort((a, b) -> SearchQueryRecord.compare(b, a, request.getMetricType()));
        }

        if (Boolean.TRUE.equals(request.getRecommendations())) {
            Map<String, List<Recommendation>> recommendationsMap = new HashMap<>();
            RecommendationService recommendationService = queryInsightsService.getRecommendationService();
            for (SearchQueryRecord record : records) {
                List<Recommendation> recs = (recommendationService != null) ? recommendationService.generateRecommendations(record) : null;
                recommendationsMap.put(record.getId(), recs != null ? recs : Collections.emptyList());
            }
            return new TopQueries(clusterService.localNode(), records, recommendationsMap);
        }
        return new TopQueries(clusterService.localNode(), records);
    }

    /**
     * Inner Node Top Queries Request
     */
    public static class NodeRequest extends TransportRequest {

        final TopQueriesRequest request;

        /**
         * Create the NodeRequest object from StreamInput
         *
         * @param in the StreamInput to read the object
         * @throws IOException IOException
         */
        public NodeRequest(StreamInput in) throws IOException {
            super(in);
            request = new TopQueriesRequest(in);
        }

        /**
         * Create the NodeRequest object from a TopQueriesRequest
         * @param request the TopQueriesRequest object
         */
        public NodeRequest(final TopQueriesRequest request) {
            this.request = request;
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(out);
            request.writeTo(out);
        }
    }
}
