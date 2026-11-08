package ai.tracewell.stream;

import ai.tracewell.stream.model.EnrichedEvent;
import ai.tracewell.stream.model.SessionRow;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Set;

/**
 * Event-time session windows per (device, identity): a session ends when the person is idle for longer than
 * the gap. The aggregate is incremental and mergeable (session windows merge as events fill the gaps), so
 * only one small accumulator per open session is kept in state, never the events themselves.
 *
 * <p>The session id is a hash of device, identity and the first event's time. The Spark backfill
 * (spark-jobs/sessions) computes the same id, so a session repaired after late events replaces the
 * stream's row by key.
 */
final class Sessions {
    private Sessions() {}

    static final List<String> BANDS = List.of("Low", "Medium", "High", "Critical");

    static String sessionId(String deviceId, String identity, long startMillis) {
        try {
            byte[] h = MessageDigest.getInstance("SHA-256")
                    .digest((deviceId + "|" + identity + "|" + startMillis).getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(h).substring(0, 16);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    /** Accumulator: a Flink POJO (public fields, no-arg constructor). */
    public static class Acc {
        public String identity;
        public String deviceId;
        public String department;
        public long start = Long.MAX_VALUE;
        public long end = Long.MIN_VALUE;
        public long count;
        public Set<String> sensors = new HashSet<>();
        public Set<String> eventTypes = new HashSet<>();
        public Set<String> assets = new HashSet<>();
        public Set<String> unknownAssets = new HashSet<>();
        public Double maxScore;
        public int maxBandRank;   // 1..4 = Low..Critical, 0 = none
        public long sensitive;
        public long blocks;
        public long warns;
        public long bytesOut;
    }

    static class Agg implements AggregateFunction<EnrichedEvent, Acc, Acc> {
        @Override
        public Acc createAccumulator() {
            return new Acc();
        }

        @Override
        public Acc add(EnrichedEvent e, Acc a) {
            a.identity = e.identity;
            a.deviceId = e.deviceId;
            a.department = e.department;
            a.start = Math.min(a.start, e.tsEvent);
            a.end = Math.max(a.end, e.tsEvent);
            a.count++;
            if (e.sensor != null) a.sensors.add(e.sensor);
            if (e.eventType != null) a.eventTypes.add(e.eventType);
            if (e.assetId != null) {
                a.assets.add(e.assetId);
                if (!Boolean.TRUE.equals(e.assetKnown)) a.unknownAssets.add(e.assetId);
            }
            if (e.riskScoreAtEvent != null && (a.maxScore == null || e.riskScoreAtEvent > a.maxScore)) a.maxScore = e.riskScoreAtEvent;
            a.maxBandRank = Math.max(a.maxBandRank, BANDS.indexOf(e.bandAtEvent) + 1);
            if (e.sensitiveCount != null && e.sensitiveCount > 0) a.sensitive++;
            if ("block".equals(e.action)) a.blocks++;
            if ("warn".equals(e.action)) a.warns++;
            if (e.bytesOut != null) a.bytesOut += e.bytesOut;
            return a;
        }

        @Override
        public Acc merge(Acc a, Acc b) {
            Acc m = new Acc();
            m.identity = a.identity != null ? a.identity : b.identity;
            m.deviceId = a.deviceId != null ? a.deviceId : b.deviceId;
            m.department = a.department != null ? a.department : b.department;
            m.start = Math.min(a.start, b.start);
            m.end = Math.max(a.end, b.end);
            m.count = a.count + b.count;
            m.sensors.addAll(a.sensors); m.sensors.addAll(b.sensors);
            m.eventTypes.addAll(a.eventTypes); m.eventTypes.addAll(b.eventTypes);
            m.assets.addAll(a.assets); m.assets.addAll(b.assets);
            m.unknownAssets.addAll(a.unknownAssets); m.unknownAssets.addAll(b.unknownAssets);
            m.maxScore = a.maxScore == null ? b.maxScore : b.maxScore == null ? a.maxScore : Math.max(a.maxScore, b.maxScore);
            m.maxBandRank = Math.max(a.maxBandRank, b.maxBandRank);
            m.sensitive = a.sensitive + b.sensitive;
            m.blocks = a.blocks + b.blocks;
            m.warns = a.warns + b.warns;
            m.bytesOut = a.bytesOut + b.bytesOut;
            return m;
        }

        @Override
        public Acc getResult(Acc a) {
            return a;
        }
    }

    static SessionRow toRow(Acc a, String source, long builtAt) {
        SessionRow r = new SessionRow();
        r.sessionId = sessionId(a.deviceId, a.identity, a.start);
        r.identity = a.identity;
        r.deviceId = a.deviceId;
        r.department = a.department;
        r.sessionStart = a.start;
        r.sessionEnd = a.end;
        r.durationS = (a.end - a.start) / 1000;
        r.eventCount = a.count;
        r.sensorsSeen = a.sensors.stream().sorted().toArray(String[]::new);
        r.eventTypes = a.eventTypes.stream().sorted().toArray(String[]::new);
        r.assetsUsed = a.assets.stream().sorted().toArray(String[]::new);
        r.unknownAssets = a.unknownAssets.size();
        r.maxRiskScore = a.maxScore;
        r.maxBand = a.maxBandRank == 0 ? null : BANDS.get(a.maxBandRank - 1);
        r.sensitiveEvents = a.sensitive;
        r.blocks = a.blocks;
        r.warns = a.warns;
        r.mbOut = Math.round(a.bytesOut / 1e4) / 100.0;
        r.source = source;
        r.builtAt = builtAt;
        return r;
    }

    static class Emit extends ProcessWindowFunction<Acc, SessionRow, String, TimeWindow> {
        @Override
        public void process(String key, Context ctx, Iterable<Acc> accs, Collector<SessionRow> out) {
            for (Acc a : accs) out.collect(toRow(a, "stream", ctx.currentProcessingTime()));
        }
    }
}
