import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.spi.v1.SpannerInterceptorProvider;
import com.google.common.collect.Lists;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import io.grpc.ForwardingClientCallListener.SimpleForwardingClientCallListener;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.opencensus.exporter.stats.stackdriver.StackdriverStatsConfiguration;
import io.opencensus.exporter.stats.stackdriver.StackdriverStatsExporter;
import io.opencensus.stats.Aggregation;
import io.opencensus.stats.BucketBoundaries;
import io.opencensus.stats.Measure.MeasureLong;
import io.opencensus.stats.Stats;
import io.opencensus.stats.StatsRecorder;
import io.opencensus.stats.View;
import io.opencensus.stats.View.Name;
import io.opencensus.stats.ViewManager;
import io.opencensus.tags.TagKey;
import io.opencensus.tags.TagValue;
import io.opencensus.tags.Tagger;
import io.opencensus.tags.Tags;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;


public class Main {

  private static final TagKey TAG = TagKey.create("directpath");
  private static final Tagger TAGGER = Tags.getTagger();
  private static final MeasureLong LATENCY_MS =
      MeasureLong.create("dp_test_latency", "The task latency in milliseconds", "ms");
  private static final BucketBoundaries LATENCY_BOUNDARIES =
      BucketBoundaries.create(
          Lists.newArrayList(0d, 1d, 2d, 3d, 4d, 5d, 6d, 7d, 8d, 9d, 10d, 20d, 30d, 40d, 50d, 60d,
              70d, 90d, 10000d));
  private static final StatsRecorder STATS_RECORDER = Stats.getStatsRecorder();

  static void flush(List<Long> stats) {
    Collections.sort(stats);
    int n = stats.size();
    System.out.println("======== STATS (ms) ======== ");
    System.out.println("MIN: " + stats.get(0));
    System.out.println("AVG: " + stats.stream().reduce(0L, Long::sum) / n);
    System.out.println("P50: " + stats.get((int) (n * 0.5)));
    System.out.println("P90: " + stats.get((int) (n * 0.9)));
    System.out.println("P99: " + stats.get((int) (n * 0.99)));
    stats.clear();
  }

  public static void main(String... args) throws Exception {
    int n = 100000;
    String instanceId = "ololo";
    String databaseId = "db";
    String projectId = "span-cloud-testing";
    View view =
        View.create(
            Name.create("dp_test_latency_distribution"),
            "The distribution of the DirectPath latencies.",
            LATENCY_MS,
            Aggregation.Distribution.create(LATENCY_BOUNDARIES),
            Collections.singletonList(TAG));
    ViewManager viewManager = Stats.getViewManager();
    viewManager.registerView(view);
    StackdriverStatsExporter.createAndRegister(StackdriverStatsConfiguration.builder()
        .setProjectId(projectId)
        .build());
    ArrayList<Long> stats = new ArrayList<>(n);

    SpannerOptions options = SpannerOptions.newBuilder()
        .setProjectId(projectId)
        // .setHost("https://wrenchworks-loadtest.googleapis.com")
        .setInterceptorProvider(
            SpannerInterceptorProvider.createDefault().with(new AddressPrinter()))
        .build();

    try (Spanner spanner = options.getService()) {
      DatabaseClient dbClient =
          spanner.getDatabaseClient(DatabaseId.of(options.getProjectId(), instanceId, databaseId));
      long counter = 0;
      while (true) {
        long start = System.nanoTime();
        ResultSet rs = dbClient.singleUse().executeQuery(Statement.of("SELECT 1"));
        rs.next();
        rs.getLong(0);
        rs.close();
        long ms = (System.nanoTime() - start) / 1000000;
        stats.add(ms);
        STATS_RECORDER.newMeasureMap().put(LATENCY_MS, ms)
            .record(TAGGER.emptyBuilder().put(TAG, TagValue.create("false")).build());
        if (++counter % n == 0) {
          flush(stats);
        }
      }
    }
  }

  static class AddressPrinter implements ClientInterceptor {

    static SocketAddress address = null;

    @Override

    public <
        ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method,
        CallOptions callOptions, Channel next) {
      final ClientCall<ReqT, RespT> clientCall = next.newCall(method, callOptions);
      return new SimpleForwardingClientCall<ReqT, RespT>(clientCall) {
        @Override
        public void start(Listener<RespT> responseListener, Metadata headers) {
          super.start(
              new SimpleForwardingClientCallListener<RespT>(responseListener) {
                @Override
                public void onHeaders(Metadata headers) {

                  SocketAddress remoteAddr =
                      clientCall.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR);
                  if (!Objects.equals(remoteAddr, address)) {
                    System.out.println("Remote addr: " + remoteAddr);
                    address = remoteAddr;
                  }
                  super.onHeaders(headers);
                }
              },
              headers);
        }
      };
    }
  }
}
// [END spanner_quickstart]
