package io.dropwizard.metrics.loganalytics;

import com.codahale.metrics.*;
import com.codahale.metrics.json.MetricsModule;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.microsoft.pnp.loganalytics.LogAnalyticsClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

public class LogAnalyticsReporter extends ScheduledReporter {

    /**
     * Returns a new {@link Builder} for {@link LogAnalyticsReporter}.
     *
     * @param registry the registry to report
     * @return a {@link Builder} instance for a {@link LogAnalyticsReporter}
     */
    public static Builder forRegistry(MetricRegistry registry) {
        return new Builder(registry);
    }

    /**
     * A builder for {@link LogAnalyticsReporter} instances. Defaults to not using a prefix, using the default clock, converting rates to
     * events/second, converting durations to milliseconds, and not filtering metrics. The default
     * Log Analytics log type is DropWizard
     */
    public static class Builder {
        private final MetricRegistry registry;
        private Clock clock;
        private String prefix;
        private TimeUnit rateUnit;
        private TimeUnit durationUnit;
        private MetricFilter filter;
        private String logType;
        private String workspaceId;
        private String workspaceKey;
        private Map<String, Object> additionalFields;

        private Builder(MetricRegistry registry) {
            this.registry = registry;
            this.clock = Clock.defaultClock();
            this.prefix = null;
            this.rateUnit = TimeUnit.SECONDS;
            this.durationUnit = TimeUnit.MILLISECONDS;
            this.filter = MetricFilter.ALL;
            this.logType = "DropWizard";
        }

        /**
         * Use the given {@link Clock} instance for the time. Usually the default clock is sufficient.
         *
         * @param clock clock
         * @return {@code this}
         */
        public Builder withClock(Clock clock) {
            this.clock = clock;
            return this;
        }

        /**
         * Configure a prefix for each metric name. Optional, but useful to identify originator of metric.
         *
         * @param prefix prefix for metric name
         * @return {@code this}
         */
        public Builder prefixedWith(String prefix) {
            this.prefix = prefix;
            return this;
        }

        /**
         * Convert all the rates to a certain TimeUnit, defaults to TimeUnit.SECONDS.
         *
         * @param rateUnit unit of rate
         * @return {@code this}
         */
        public Builder convertRatesTo(TimeUnit rateUnit) {
            this.rateUnit = rateUnit;
            return this;
        }

        /**
         * Convert all the durations to a certain TimeUnit, defaults to TimeUnit.MILLISECONDS
         *
         * @param durationUnit unit of duration
         * @return {@code this}
         */
        public Builder convertDurationsTo(TimeUnit durationUnit) {
            this.durationUnit = durationUnit;
            return this;
        }

        /**
         * Allows to configure a special MetricFilter, which defines what metrics are reported
         *
         * @param filter metrics filter
         * @return {@code this}
         */
        public Builder filter(MetricFilter filter) {
            this.filter = filter;
            return this;
        }

        /**
         * The log type to send to Log Analytics. Defaults to 'DropWizard'.
         *
         * @param logType Log Analytics log type
         * @return {@code this}
         */
        public Builder withLogType(String logType) {
            this.logType = logType;
            return this;
        }

        /**
         * The workspace id of the Log Analytics workspace
         *
         * @param workspaceId Log Analytics workspace id
         * @return {@code this}
         */
        public Builder withWorkspaceId(String workspaceId) {
            this.workspaceId = workspaceId;
            return this;
        }

        /**
         * The workspace key of the Log Analytics workspace
         *
         * @param workspaceKey Log Analytics workspace key
         * @return {@code this}
         */
        public Builder withWorkspaceKey(String workspaceKey) {
            this.workspaceKey = workspaceKey;
            return this;
        }

        /**
         * Additional fields to be included for each metric
         *
         * @param additionalFields custom fields for reporting
         * @return {@code this}
         */
        public Builder additionalFields(Map<String, Object> additionalFields) {
            this.additionalFields = additionalFields;
            return this;
        }

        /**
         * Builds a {@link LogAnalyticsReporter} with the given properties.
         *
         * @return a {@link LogAnalyticsReporter}
         */
        public LogAnalyticsReporter build() {
            return new LogAnalyticsReporter(registry,
                    workspaceId,
                    workspaceKey,
                    logType,
                    clock,
                    prefix,
                    rateUnit,
                    durationUnit,
                    filter,
                    additionalFields);
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(LogAnalyticsReporter.class);

    private final String workspaceId;
    private final String workspaceKey;
    private final String logType;
    private final Clock clock;
    private final String prefix;
    private Map<String, Object> additionalFields;
    private ObjectMapper mapper;
    private LogAnalyticsClient logAnalyticsClient;
    public LogAnalyticsReporter(MetricRegistry registry, String workspaceId, String workspaceKey,
                                String logType,
                                Clock clock, String prefix, TimeUnit rateUnit, TimeUnit durationUnit,
                                MetricFilter filter, Map<String, Object> additionalFields) {
        super(registry, "loganalytics-reporter", filter, rateUnit, durationUnit);
        this.workspaceId = workspaceId;
        this.workspaceKey = workspaceKey;
        this.logType = logType;
        this.clock = clock;
        this.prefix = prefix;
        this.additionalFields = additionalFields;
        this.mapper = new ObjectMapper().registerModule(new MetricsModule(rateUnit,
                durationUnit,
                true,
                filter));
        this.logAnalyticsClient = new LogAnalyticsClient(
                this.workspaceId,
                this.workspaceKey);
    }

    @Override
    public void report(SortedMap<String, Gauge> gauges,
                       SortedMap<String, Counter> counters,
                       SortedMap<String, Histogram> histograms,
                       SortedMap<String, Meter> meters,
                       SortedMap<String, Timer> timers) {

        ArrayList<JsonNode> nodes = new ArrayList<JsonNode>();
        // nothing to do if we don't have any metrics to report
        if (gauges.isEmpty() && counters.isEmpty() && histograms.isEmpty() && meters.isEmpty() && timers.isEmpty()) {
            LOGGER.info("All metrics empty, nothing to report");
            return;
        }

        for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
            if (entry.getValue().getValue() != null) {
                nodes.add(addProperties(entry.getKey(), entry.getValue()));
            }
        }

        for (Map.Entry<String, Counter> entry : counters.entrySet()) {
            nodes.add(addProperties(entry.getKey(), entry.getValue()));
        }

        for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
            nodes.add(addProperties(entry.getKey(), entry.getValue()));
        }

        for (Map.Entry<String, Meter> entry : meters.entrySet()) {
            nodes.add(addProperties(entry.getKey(), entry.getValue()));
        }

        for (Map.Entry<String, Timer> entry : timers.entrySet()) {
            nodes.add(addProperties(entry.getKey(), entry.getValue()));
        }

        try {
            String json = this.mapper.writer().writeValueAsString(nodes);
            this.logAnalyticsClient.send(json, this.logType);
        } catch (IOException ioe) {
            LOGGER.warn("Error reporting metrics", ioe);
        }
    }

    private JsonNode addProperties(final String name, final Metric metric) {
        String metricType;
        if (metric instanceof Counter) {
            metricType = Counter.class.getSimpleName();
        } else if (metric instanceof Gauge) {
            metricType = Gauge.class.getSimpleName();
        } else if (metric instanceof Histogram) {
            metricType = Histogram.class.getSimpleName();
        } else if (metric instanceof Meter) {
            metricType = Meter.class.getSimpleName();
        } else if (metric instanceof Timer) {
            metricType = Timer.class.getSimpleName();
        } else {
            throw new IllegalArgumentException("Unsupported metric type");
        }
        ObjectNode node = this.mapper.valueToTree(metric);
        node.put("metric_type", metricType);
        node.put("name", name);
        return node;
    }
}
