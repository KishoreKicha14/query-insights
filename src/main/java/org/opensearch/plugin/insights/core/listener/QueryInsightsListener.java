/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.insights.core.listener;

import static org.opensearch.plugin.insights.rules.model.SearchQueryRecord.DEFAULT_TOP_N_QUERY_MAP;
import static org.opensearch.plugin.insights.settings.QueryCategorizationSettings.SEARCH_QUERY_METRICS_ENABLED_SETTING;
import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.TOP_N_QUERIES_EXCLUDED_INDICES;
import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.TOP_N_QUERIES_GROUPING_FIELD_NAME;
import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.TOP_N_QUERIES_GROUPING_FIELD_TYPE;
import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.TOP_N_QUERIES_GROUP_BY;
import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.TOP_N_QUERIES_MAX_GROUPS_EXCLUDING_N;
import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.getTopNEnabledSetting;
import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.getTopNSizeSetting;
import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.getTopNWindowSizeSetting;

import java.time.Instant;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.SearchPhaseContext;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchRequestContext;
import org.opensearch.action.search.SearchRequestOperationsListener;
import org.opensearch.action.search.SearchTask;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.index.Index;
import org.opensearch.core.tasks.resourcetracker.TaskResourceInfo;
import org.opensearch.plugin.insights.core.metrics.OperationalMetric;
import org.opensearch.plugin.insights.core.metrics.OperationalMetricsCounter;
import org.opensearch.plugin.insights.core.service.QueryInsightsService;
import org.opensearch.plugin.insights.core.service.categorizer.QueryShapeGenerator;
import org.opensearch.plugin.insights.rules.model.Attribute;
import org.opensearch.plugin.insights.rules.model.Measurement;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tasks.Task;
import reactor.util.annotation.NonNull;
import java.util.ArrayList;
import java.util.Comparator;


public final class QueryInsightsListener extends SearchRequestOperationsListener {

    private static final Logger log = LogManager.getLogger(QueryInsightsListener.class);

    private final QueryInsightsService queryInsightsService;
    private final ClusterService clusterService;
    private boolean groupingFieldNameEnabled;
    private boolean groupingFieldTypeEnabled;
    private final QueryShapeGenerator queryShapeGenerator;
    private Set<Pattern> excludedIndicesPattern;

    @Inject
    public QueryInsightsListener(final ClusterService clusterService, final QueryInsightsService queryInsightsService) {
        this(clusterService, queryInsightsService, false);
        groupingFieldNameEnabled = false;
        groupingFieldTypeEnabled = false;
    }

    public QueryInsightsListener(
        final ClusterService clusterService,
        final QueryInsightsService queryInsightsService,
        boolean initiallyEnabled
    ) {
        super(initiallyEnabled);
        this.clusterService = clusterService;
        this.queryInsightsService = queryInsightsService;
        this.queryShapeGenerator = new QueryShapeGenerator(clusterService);
        queryInsightsService.setQueryShapeGenerator(queryShapeGenerator);

        for (MetricType type : MetricType.allMetricTypes()) {
            clusterService.getClusterSettings()
                .addSettingsUpdateConsumer(getTopNEnabledSetting(type), v -> this.setEnableTopQueries(type, v));
            clusterService.getClusterSettings()
                .addSettingsUpdateConsumer(
                    getTopNSizeSetting(type),
                    v -> this.queryInsightsService.setTopNSize(type, v),
                    v -> this.queryInsightsService.validateTopNSize(type, v)
                );
            clusterService.getClusterSettings()
                .addSettingsUpdateConsumer(
                    getTopNWindowSizeSetting(type),
                    v -> this.queryInsightsService.setWindowSize(type, v),
                    v -> this.queryInsightsService.validateWindowSize(type, v)
                );

            this.setEnableTopQueries(type, clusterService.getClusterSettings().get(getTopNEnabledSetting(type)));
            this.queryInsightsService.validateTopNSize(type, clusterService.getClusterSettings().get(getTopNSizeSetting(type)));
            this.queryInsightsService.setTopNSize(type, clusterService.getClusterSettings().get(getTopNSizeSetting(type)));
            this.queryInsightsService.validateWindowSize(type, clusterService.getClusterSettings().get(getTopNWindowSizeSetting(type)));
            this.queryInsightsService.setWindowSize(type, clusterService.getClusterSettings().get(getTopNWindowSizeSetting(type)));
        }

        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(
                TOP_N_QUERIES_GROUP_BY,
                v -> this.queryInsightsService.setGrouping(v),
                v -> this.queryInsightsService.validateGrouping(v)
            );
        this.queryInsightsService.validateGrouping(clusterService.getClusterSettings().get(TOP_N_QUERIES_GROUP_BY));
        this.queryInsightsService.setGrouping(clusterService.getClusterSettings().get(TOP_N_QUERIES_GROUP_BY));

        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(
                TOP_N_QUERIES_MAX_GROUPS_EXCLUDING_N,
                v -> this.queryInsightsService.setMaximumGroups(v),
                v -> this.queryInsightsService.validateMaximumGroups(v)
            );
        this.queryInsightsService.validateMaximumGroups(clusterService.getClusterSettings().get(TOP_N_QUERIES_MAX_GROUPS_EXCLUDING_N));
        this.queryInsightsService.setMaximumGroups(clusterService.getClusterSettings().get(TOP_N_QUERIES_MAX_GROUPS_EXCLUDING_N));

        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(
                QueryInsightsSettings.TOP_N_QUERIES_EXCLUDED_INDICES,
                this::setExcludedIndices,
                this::validateExcludedIndices
            );
        validateExcludedIndices(clusterService.getClusterSettings().get(TOP_N_QUERIES_EXCLUDED_INDICES));
        setExcludedIndices(clusterService.getClusterSettings().get(TOP_N_QUERIES_EXCLUDED_INDICES));

        clusterService.getClusterSettings().addSettingsUpdateConsumer(TOP_N_QUERIES_GROUPING_FIELD_NAME, this::setGroupingFieldNameEnabled);
        setGroupingFieldNameEnabled(clusterService.getClusterSettings().get(TOP_N_QUERIES_GROUPING_FIELD_NAME));

        clusterService.getClusterSettings().addSettingsUpdateConsumer(TOP_N_QUERIES_GROUPING_FIELD_TYPE, this::setGroupingFieldTypeEnabled);
        setGroupingFieldTypeEnabled(clusterService.getClusterSettings().get(TOP_N_QUERIES_GROUPING_FIELD_TYPE));

        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(SEARCH_QUERY_METRICS_ENABLED_SETTING, this::setSearchQueryMetricsEnabled);
        setSearchQueryMetricsEnabled(clusterService.getClusterSettings().get(SEARCH_QUERY_METRICS_ENABLED_SETTING));

        log.info("[INSIGHTS][coord] QueryInsightsListener initialized; enabled={}", super.isEnabled());
    }

    private void setExcludedIndices(List<String> excludedIndices) {
        this.excludedIndicesPattern = excludedIndices.stream()
            .map(index -> index.contains("*") ? index.replace("*", ".*") : index)
            .map(Pattern::compile)
            .collect(Collectors.toSet());
    }

    public void setEnableTopQueries(final MetricType metricType, final boolean isCurrentMetricEnabled) {
        this.queryInsightsService.enableCollection(metricType, isCurrentMetricEnabled);
        log.info("[INSIGHTS][coord] topN toggle metric={} enabled={}", metricType, isCurrentMetricEnabled);
        updateQueryInsightsState();
    }

    public void setSearchQueryMetricsEnabled(boolean searchQueryMetricsEnabled) {
        this.queryInsightsService.enableSearchQueryMetricsFeature(searchQueryMetricsEnabled);
        updateQueryInsightsState();
    }

    public void setGroupingFieldNameEnabled(Boolean fieldNameEnabled) { this.groupingFieldNameEnabled = fieldNameEnabled; }

    public void setGroupingFieldTypeEnabled(Boolean fieldTypeEnabled) { this.groupingFieldTypeEnabled = fieldTypeEnabled; }

    private void updateQueryInsightsState() {
        boolean anyFeatureEnabled = queryInsightsService.isAnyFeatureEnabled();
        if (anyFeatureEnabled && !super.isEnabled()) {
            super.setEnabled(true);
            queryInsightsService.stop();
            queryInsightsService.start();
        } else if (!anyFeatureEnabled && super.isEnabled()) {
            super.setEnabled(false);
            queryInsightsService.stop();
        }
    }

    @Override
    public boolean isEnabled() { return super.isEnabled(); }

    private static String phaseName(SearchPhaseContext ctx) {
        try { return String.valueOf(ctx.getCurrentPhase().getName()); } catch (Throwable t) { return "unknown"; }
    }

    @Override
    public void onPhaseStart(SearchPhaseContext context) {
        if (!queryInsightsService.isAnyFeatureEnabled()) return;
        final String phase = phaseName(context);
        final long reqId = context.getTask().getId();

        var starts = new ArrayList<>(queryInsightsService.snapshotOpenStarts(reqId, phase));
        starts.sort(Comparator.comparing(s -> s.shardId.toString()));
    }

    @Override
    public void onPhaseEnd(SearchPhaseContext context, SearchRequestContext searchRequestContext) {
        if (!queryInsightsService.isAnyFeatureEnabled()) return;
        final String phase = phaseName(context);
        final long reqId = context.getTask().getId();
        var spans = new ArrayList<>(queryInsightsService.snapshotShardPhaseSpans(reqId, phase));
        spans.sort(Comparator.comparing(sp -> sp.shardId.toString()));
    }

    @Override
    public void onPhaseFailure(SearchPhaseContext context, Throwable cause) {
        if (!queryInsightsService.isAnyFeatureEnabled()) return;
        final String phase = phaseName(context);
        final long reqId = context.getTask().getId();
        var spans = queryInsightsService.drainShardPhaseSpans(reqId, phase);
    }

    @Override
    public void onRequestStart(SearchRequestContext searchRequestContext) {}

    @Override
    public void onRequestEnd(final SearchPhaseContext context, final SearchRequestContext searchRequestContext) {
        constructSearchQueryRecord(context, searchRequestContext);
        queryInsightsService.clearForRequest(context.getTask().getId());
    }

    @Override
    public void onRequestFailure(final SearchPhaseContext context, final SearchRequestContext searchRequestContext) {
        constructSearchQueryRecord(context, searchRequestContext);
    }

    private boolean skipSearchRequest(final SearchRequestContext searchRequestContext) {
        if (Optional.ofNullable(searchRequestContext)
            .map(SearchRequestContext::getRequest)
            .map(SearchRequest::source)
            .map(SearchSourceBuilder::profile)
            .orElse(false)) {
            return true;
        }

        if (excludedIndicesPattern.isEmpty()) {
            return false;
        }

        boolean skip = Optional.ofNullable(searchRequestContext)
            .map(SearchRequestContext::getSuccessfulSearchShardIndices)
            .map(indices -> indices.stream().map(Index::getName).anyMatch(this::matchedExcludedIndices))
            .orElse(false);

        if (skip) {
            log.debug("[INSIGHTS][coord] skip record: excluded indices matched");
        }
        return skip;
    }

    private boolean matchedExcludedIndices(String indexName) {
        if (indexName == null || excludedIndicesPattern == null) { return false; }
        return excludedIndicesPattern.stream().anyMatch(pattern -> pattern.matcher(indexName).matches());
    }

    private void constructSearchQueryRecord(final SearchPhaseContext context, final SearchRequestContext searchRequestContext) {
        if (skipSearchRequest(searchRequestContext)) { return; }

        SearchTask searchTask = context.getTask();
        List<TaskResourceInfo> tasksResourceUsages = searchRequestContext.getPhaseResourceUsage();
        tasksResourceUsages.add(
            new TaskResourceInfo(
                searchTask.getAction(),
                searchTask.getId(),
                searchTask.getParentTaskId().getId(),
                clusterService.localNode().getId(),
                searchTask.getTotalResourceStats()
            )
        );

        final SearchRequest request = context.getRequest();
        try {
            Map<MetricType, Measurement> measurements = new HashMap<>();
            long tookMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - searchRequestContext.getAbsoluteStartNanos());
            measurements.put(MetricType.LATENCY, new Measurement(tookMs));
            long cpu = tasksResourceUsages.stream().map(a -> a.getTaskResourceUsage().getCpuTimeInNanos()).mapToLong(Long::longValue).sum();
            long mem = tasksResourceUsages.stream().map(a -> a.getTaskResourceUsage().getMemoryInBytes()).mapToLong(Long::longValue).sum();
            measurements.put(MetricType.CPU, new Measurement(cpu));
            measurements.put(MetricType.MEMORY, new Measurement(mem));

            Map<Attribute, Object> attributes = new HashMap<>();
            attributes.put(Attribute.SEARCH_TYPE, request.searchType().toString().toLowerCase(Locale.ROOT));
            attributes.put(Attribute.SOURCE, request.source());
            attributes.put(Attribute.TOTAL_SHARDS, context.getNumShards());
            attributes.put(Attribute.INDICES, request.indices());
            attributes.put(Attribute.PHASE_LATENCY_MAP, searchRequestContext.phaseTookMap());
            attributes.put(Attribute.TASK_RESOURCE_USAGES, tasksResourceUsages);
            attributes.put(Attribute.GROUP_BY, QueryInsightsSettings.DEFAULT_GROUPING_TYPE);
            attributes.put(Attribute.NODE_ID, clusterService.localNode().getId());
            attributes.put(Attribute.TOP_N_QUERY, new HashMap<>(DEFAULT_TOP_N_QUERY_MAP));

            if (queryInsightsService.isGroupingEnabled() || log.isTraceEnabled()) {
                final String queryShape = queryShapeGenerator.buildShape(
                    request.source(),
                    groupingFieldNameEnabled,
                    groupingFieldTypeEnabled,
                    searchRequestContext.getSuccessfulSearchShardIndices()
                );
                if (log.isTraceEnabled()) {
                    log.trace("[INSIGHTS][coord] Query Shape:\n{}", queryShape);
                }
                if (queryInsightsService.isGroupingEnabled()) {
                    String hashcode = queryShapeGenerator.getShapeHashCodeAsString(queryShape);
                    attributes.put(Attribute.QUERY_GROUP_HASHCODE, hashcode);
                }
            }

            Map<String, Object> labels = new HashMap<>();
            String userProvidedLabel = context.getTask().getHeader(Task.X_OPAQUE_ID);
            if (userProvidedLabel != null) { labels.put(Task.X_OPAQUE_ID, userProvidedLabel); }
            attributes.put(Attribute.LABELS, labels);

            SearchQueryRecord record = new SearchQueryRecord(request.getOrCreateAbsoluteStartMillis(), measurements, attributes);
            queryInsightsService.addRecord(record);

            log.info("[INSIGHTS][coord] RECORDED reqId={} tookMs={} cpuNanos={} memBytes={}",
                context.getTask().getId(), tookMs, cpu, mem);
        } catch (Exception e) {
            OperationalMetricsCounter.getInstance().incrementCounter(OperationalMetric.DATA_INGEST_EXCEPTIONS);
            log.error(String.format(Locale.ROOT, "fail to ingest query insight data, error: %s", e));
        }
    }

    public void validateExcludedIndices(@NonNull List<String> excludedIndices) {
        for (String index : excludedIndices) {
            if (index == null) { throw new IllegalArgumentException("Excluded index name cannot be null."); }
            if (index.isBlank()) { throw new IllegalArgumentException("Excluded index name cannot be blank."); }
            if (index.chars().anyMatch(Character::isUpperCase)) {
                throw new IllegalArgumentException("Index name must be lowercase.");
            }
        }
    }
}
