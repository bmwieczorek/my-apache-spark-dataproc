package com.codahale.metrics;

import com.google.api.Metric;
import com.google.api.MonitoredResource;
import com.google.cloud.monitoring.v3.MetricServiceClient;
import com.google.monitoring.v3.*;
import com.google.protobuf.util.Timestamps;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MyGcpMetricsReporter extends ScheduledReporter {

    public static Builder forRegistry(MetricRegistry registry) {
        return new Builder(registry);
    }

    public static class Builder {
        private final MetricRegistry registry;
        private MetricServiceClient metricServiceClient;
        private String projectId;
        private TimeUnit rateUnit;
        private TimeUnit durationUnit;
        private MetricFilter filter;
        private ScheduledExecutorService executor;
        private boolean shutdownExecutorOnStop;
        private Set<MetricAttribute> disabledMetricAttributes;

        private Builder(MetricRegistry registry) {
            this.registry = registry;
            this.metricServiceClient = getMetricServiceClient();
            this.projectId = "sab-dev-dap-data-pipeline-3013";
            this.rateUnit = TimeUnit.SECONDS;
            this.durationUnit = TimeUnit.MILLISECONDS;
            this.filter = MetricFilter.ALL;
            this.executor = null;
            this.shutdownExecutorOnStop = true;
            disabledMetricAttributes = Collections.emptySet();
        }

        private static MetricServiceClient getMetricServiceClient() {
            try {
                return MetricServiceClient.create();
            } catch (IOException e) {
                System.out.println("Exception creating MetricServiceClient error message " + e.getMessage());
                System.out.println("Exception creating MetricServiceClient error cause " + getNestedCause(e));
                System.out.println("Exception creating MetricServiceClient error stacktrace " + getNestedStacktrace(e));
                throw new RuntimeException(e);
            }
        }

        public Builder shutdownExecutorOnStop(boolean shutdownExecutorOnStop) {
            this.shutdownExecutorOnStop = shutdownExecutorOnStop;
            return this;
        }

        public Builder scheduleOn(ScheduledExecutorService executor) {
            this.executor = executor;
            return this;
        }

        public Builder metricServiceClient(MetricServiceClient metricServiceClient) {
            this.metricServiceClient = metricServiceClient;
            return this;
        }

        public Builder projectId(String projectId) {
            this.projectId = projectId;
            return this;
        }

        public Builder convertRatesTo(TimeUnit rateUnit) {
            this.rateUnit = rateUnit;
            return this;
        }

        public Builder convertDurationsTo(TimeUnit durationUnit) {
            this.durationUnit = durationUnit;
            return this;
        }

        public Builder filter(MetricFilter filter) {
            this.filter = filter;
            return this;
        }

        public Builder disabledMetricAttributes(Set<MetricAttribute> disabledMetricAttributes) {
            this.disabledMetricAttributes = disabledMetricAttributes;
            return this;
        }

        public MyGcpMetricsReporter build() {
            return new MyGcpMetricsReporter(registry,
                    projectId,
                    metricServiceClient,
                    rateUnit,
                    durationUnit,
                    filter,
                    executor,
                    shutdownExecutorOnStop,
                    disabledMetricAttributes);
        }
    }

    private final String projectId;
    private final MetricServiceClient metricServiceClient;

    private MyGcpMetricsReporter(MetricRegistry registry,
                                 String projectId,
                                 MetricServiceClient metricServiceClient,
                                 TimeUnit rateUnit,
                                 TimeUnit durationUnit,
                                 MetricFilter filter,
                                 ScheduledExecutorService executor,
                                 boolean shutdownExecutorOnStop,
                                 Set<MetricAttribute> disabledMetricAttributes) {
        super(registry, "gcp-custom-metrics-reporter", filter, rateUnit, durationUnit, executor, shutdownExecutorOnStop, disabledMetricAttributes);
        this.projectId = projectId;
        this.metricServiceClient = metricServiceClient;
    }

    @Override
    public void report(SortedMap<String, Gauge> gauges,
                       SortedMap<String, Counter> counters,
                       SortedMap<String, Histogram> histograms,
                       SortedMap<String, Meter> meters,
                       SortedMap<String, Timer> timers) {

        Map<String, String> resourceLabels = new HashMap<>();
        String instanceId = httpGetOrDefault("http://metadata.google.internal/computeMetadata/v1/instance/id", "000000000000000000");
        resourceLabels.put("instance_id", instanceId);
        String instanceZoneWithProject = httpGetOrDefault("http://metadata.google.internal/computeMetadata/v1/instance/zone", "us-central1-b");
        String[] instanceZoneWithProjectSplit = instanceZoneWithProject.split("/");
        String zone = instanceZoneWithProjectSplit[instanceZoneWithProjectSplit.length - 1];
        resourceLabels.put("zone", zone);

        if (!counters.isEmpty()) {
            for (Map.Entry<String, Counter> entry : counters.entrySet()) {
                if (entry.getKey().contains("plugin")) {
                    String key = entry.getKey();
                    long value = entry.getValue().getCount();
                    String metricType = "custom.googleapis.com/spark/" + updateMetricName(key);
                    System.out.println("Publishing " + metricType + "=" + value);
                    String resourceType = "gce_instance";


                    try {
                        CreateTimeSeriesRequest request = getCreateTimeSeriesRequest(value, projectId, metricType, resourceType, resourceLabels);
                        metricServiceClient.createTimeSeries(request);
                    } catch (Exception e) {
                        System.out.println("Exception publishing metric error message " + e.getMessage());
                        System.out.println("Exception publishing metric error cause " + getNestedCause(e));
                        System.out.println("Exception publishing metric error stacktrace " + getNestedStacktrace(e));
                    }
                }
            }
        }
    }

    private static String httpGetOrDefault(String uri, String defaultValue) {
        try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
            HttpGet request = new HttpGet(uri);
            request.addHeader("Metadata-Flavor", "Google");
            CloseableHttpResponse response = httpclient.execute(request);
            return EntityUtils.toString(response.getEntity());
        } catch (IOException e) {
            System.out.println("Exception sending http get request for " + uri + " error message " + e.getMessage());
            System.out.println("Exception sending http get request for " + uri + " error cause " + getNestedCause(e));
            System.out.println("Exception sending http get request for " + uri + " error stacktrace " + getNestedStacktrace(e));
        }
        return defaultValue;
    }

    private CreateTimeSeriesRequest getCreateTimeSeriesRequest(long value, String projectId, String metricType, String resourceType, Map<String, String> resourceLabels) {
        // Prepares an individual data point
        TimeInterval interval =
                TimeInterval.newBuilder()
                        .setEndTime(Timestamps.fromMillis(System.currentTimeMillis()))
                        .build();
        TypedValue typedValue = TypedValue.newBuilder().setInt64Value(value).build();
        Point point = Point.newBuilder().setInterval(interval).setValue(typedValue).build();

        List<Point> pointList = new ArrayList<>();
        pointList.add(point);

        ProjectName name = ProjectName.of(projectId);

// Prepares the metric descriptor
        Map<String, String> metricLabels = new HashMap<>();
        com.google.api.Metric metric =
                Metric.newBuilder()
                        .setType(metricType)
                        .putAllLabels(metricLabels)
                        .build();

        // Prepares the monitored resource descriptor
        MonitoredResource resource =
                MonitoredResource.newBuilder().setType(resourceType).putAllLabels(resourceLabels).build();

        // Prepares the time series request
        TimeSeries timeSeries =
                TimeSeries.newBuilder()
                        .setMetric(metric)
                        .setResource(resource)
                        .addAllPoints(pointList)
                        .build();

        List<TimeSeries> timeSeriesList = new ArrayList<>();
        timeSeriesList.add(timeSeries);

        return CreateTimeSeriesRequest.newBuilder()
                .setName(name.toString())
                .addAllTimeSeries(timeSeriesList)
                .build();
    }

    private static String getNestedCause(Exception e) {
        List<String> nestedCause = new ArrayList<>();
        Throwable cause = e.getCause();
        while (cause != null) {
            nestedCause.add(cause.toString());
            cause = cause.getCause();
        }
        return nestedCause.toString();
    }

    private static String getNestedStacktrace(Exception e) {
        return Stream.of(e.getStackTrace()).map(StackTraceElement::toString).collect(Collectors.toList()).toString();
    }

    private static String updateMetricName(String originalMetricName) {
        String[] split = originalMetricName.split("\\.");
        String[] pluginClassSplit = split[split.length - 2].split("\\$");
        return String.format("%s.%s.%s.%s", split[0], split[1], pluginClassSplit[pluginClassSplit.length - 1], split[split.length - 1]);
    }
}
