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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MyGcpMetricsReporter extends ScheduledReporter {
    private static final Logger LOGGER = LoggerFactory.getLogger(MyGcpMetricsReporter.class);

    private final String projectId;
    private final MetricServiceClient metricServiceClient;

    public MyGcpMetricsReporter(MetricRegistry registry) {
        super(registry, "gcp-custom-metrics-reporter", MetricFilter.ALL, TimeUnit.SECONDS, TimeUnit.MILLISECONDS, null, true, Collections.emptySet());
        this.projectId = System.getenv("GCP_PROJECT") != null ? System.getenv("GCP_PROJECT") : httpGetOrDefault("http://metadata.google.internal/computeMetadata/v1/project/project-id");
        this.metricServiceClient = getMetricServiceClient();
    }

    @Override
    public void report(SortedMap<String, Gauge> gauges,
                       SortedMap<String, Counter> counters,
                       SortedMap<String, Histogram> histograms,
                       SortedMap<String, Meter> meters,
                       SortedMap<String, Timer> timers) {

        Map<String, String> resourceLabels = new HashMap<>();
        String instanceId =  System.getenv("GCP_PROJECT") != null ? "1234566789123456789" : httpGetOrDefault("http://metadata.google.internal/computeMetadata/v1/instance/id");
        resourceLabels.put("instance_id", instanceId);
        String instanceZoneWithProject = System.getenv("GCP_PROJECT") != null ? "project/" + System.getenv("GCP_PROJECT") + "/zones/" + System.getenv("GCP_ZONE") : httpGetOrDefault("http://metadata.google.internal/computeMetadata/v1/instance/zone");
        String[] instanceZoneWithProjectSplit = instanceZoneWithProject.split("/");
        String zone = instanceZoneWithProjectSplit[instanceZoneWithProjectSplit.length - 1];
        resourceLabels.put("zone", zone);

        if (!counters.isEmpty()) {
            for (Map.Entry<String, Counter> entry : counters.entrySet()) {
                if (entry.getKey().contains("plugin")) {
                    String key = entry.getKey();
                    long value = entry.getValue().getCount();
                    String metricType = "custom.googleapis.com/spark/" + updateMetricName(key);
                    LOGGER.info(getThreadInfo() + " Publishing " + metricType + "=" + value);
                    String resourceType = "gce_instance";
                    try {
                        CreateTimeSeriesRequest request = getCreateTimeSeriesRequest(value, projectId, metricType, resourceType, resourceLabels);
                        metricServiceClient.createTimeSeries(request);
                    } catch (Exception e) {
                        LOGGER.warn(getThreadInfo() + " Exception publishing GCP metric " + metricType + ", message: " + e.getMessage());
//                        LOGGER.warn(" Exception publishing GCP custom metric, cause: " + getNestedCause(e));
//                        LOGGER.warn(" Exception publishing GCP custom metric, stacktrace: " + getNestedStacktrace(e));
//                        LOGGER.warn(" Exception publishing GCP custom metric", e);
                    }
                }
            }
        }
    }

    private static String getThreadInfo() {
        return "[" + Thread.currentThread().getName() + ":" + Thread.currentThread().getId() + "]";
    }

    private static MetricServiceClient getMetricServiceClient() {
        try {
            return MetricServiceClient.create();
        } catch (IOException e) {
            LOGGER.error("Exception creating MetricServiceClient, message: " + e.getMessage());
            LOGGER.error("Exception creating MetricServiceClient, cause: " + getNestedCause(e));
            LOGGER.error("Exception creating MetricServiceClient, stacktrace: " + getNestedStacktrace(e));
            LOGGER.error("Exception creating MetricServiceClient", e);
            throw new RuntimeException(e);
        }
    }

    private static String httpGetOrDefault(String uri) {
        try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
            HttpGet request = new HttpGet(uri);
            request.addHeader("Metadata-Flavor", "Google");
            CloseableHttpResponse response = httpclient.execute(request);
            return EntityUtils.toString(response.getEntity());
        } catch (IOException e) {
            LOGGER.error("Exception sending http get request for " + uri + ", message: " + e.getMessage());
            LOGGER.error("Exception sending http get request for " + uri + ", cause: " + getNestedCause(e));
            LOGGER.error("Exception sending http get request for " + uri + ", stacktrace: " + getNestedStacktrace(e));
            LOGGER.error("Exception sending http get request for " + uri, e);
            throw new RuntimeException(e);
        }
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
