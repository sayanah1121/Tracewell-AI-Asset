package ai.tracewell.stream;

import ai.tracewell.stream.model.AiEvent;
import ai.tracewell.stream.model.AssetRisk;
import ai.tracewell.stream.model.ControlMessage;
import ai.tracewell.stream.model.EnrichedEvent;
import ai.tracewell.stream.model.PolicyRule;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Keyed by device_id, connected to the broadcast control stream (risk catalog + policies).
 *
 * <ul>
 *   <li>Broadcast side: keeps every asset's current risk profile, an alias index (any name a sensor may
 *       report -> asset_id) and the policy rules. Tombstones remove assets/policies. Updates apply to the
 *       next event, with no restart.</li>
 *   <li>Event side: resolves asset_ref through the alias index, evaluates policies (priority order, first
 *       match wins, default allow) and emits an EnrichedEvent. Unknown assets get a provisional High profile
 *       and are reported once per device on the new-assets side output.</li>
 *   <li>Warm-up: after a (re)start the catalog is replayed from the compacted topic. Events that arrive
 *       before it has loaded are parked in keyed state and released by a timer, so they are not wrongly
 *       judged "unknown".</li>
 * </ul>
 */
public class EnrichAndDecide extends KeyedBroadcastProcessFunction<String, AiEvent, ControlMessage, EnrichedEvent> {

    public static final MapStateDescriptor<String, AssetRisk> ASSETS =
            new MapStateDescriptor<>("assets", Types.STRING, TypeInformation.of(AssetRisk.class));
    public static final MapStateDescriptor<String, String> ALIASES =
            new MapStateDescriptor<>("aliases", Types.STRING, Types.STRING);
    public static final MapStateDescriptor<String, PolicyRule> POLICIES =
            new MapStateDescriptor<>("policies", Types.STRING, TypeInformation.of(PolicyRule.class));

    public static final OutputTag<String> NEW_ASSETS = new OutputTag<>("new-assets", Types.STRING);

    private static final long WARMUP_QUIET_MS = 3_000;   // catalog is "loaded" once no update arrived for 3 s
    private static final long RETRY_MS = 1_000;

    private transient ListState<AiEvent> parked;
    private transient MapState<String, Boolean> reportedUnknown;
    private transient long lastControlAt;
    private transient boolean catalogLoaded;
    private transient List<PolicyRule> sortedPolicies;
    private transient Counter unknownAssets;
    private transient Counter parkedEvents;

    @Override
    public void open(Configuration parameters) {
        parked = getRuntimeContext().getListState(new ListStateDescriptor<>("parked", TypeInformation.of(AiEvent.class)));
        MapStateDescriptor<String, Boolean> unknown = new MapStateDescriptor<>("reported-unknown", Types.STRING, Types.BOOLEAN);
        unknown.enableTimeToLive(StateTtlConfig.newBuilder(Duration.ofDays(1)).build());
        reportedUnknown = getRuntimeContext().getMapState(unknown);
        unknownAssets = getRuntimeContext().getMetricGroup().counter("unknown_assets");
        parkedEvents = getRuntimeContext().getMetricGroup().counter("events_parked_during_warmup");
        lastControlAt = System.currentTimeMillis();
    }

    // ───────────────────────────── broadcast side ─────────────────────────────

    @Override
    public void processBroadcastElement(ControlMessage m, Context ctx, Collector<EnrichedEvent> out) throws Exception {
        lastControlAt = ctx.currentProcessingTime();
        if (ControlMessage.POLICY.equals(m.kind)) {
            BroadcastState<String, PolicyRule> policies = ctx.getBroadcastState(POLICIES);
            if (m.delete) policies.remove(m.key); else policies.put(m.key, m.policy);
            sortedPolicies = null;
            return;
        }
        BroadcastState<String, AssetRisk> assets = ctx.getBroadcastState(ASSETS);
        BroadcastState<String, String> aliases = ctx.getBroadcastState(ALIASES);
        AssetRisk previous = assets.get(m.key);
        if (previous != null) {
            for (String a : previous.aliases) aliases.remove(normalize(a));
        }
        if (m.delete) {
            assets.remove(m.key);
            return;
        }
        assets.put(m.key, m.asset);
        aliases.put(normalize(m.asset.assetId), m.asset.assetId);
        for (String a : m.asset.aliases) aliases.put(normalize(a), m.asset.assetId);
    }

    // ───────────────────────────── event side ─────────────────────────────

    @Override
    public void processElement(AiEvent e, ReadOnlyContext ctx, Collector<EnrichedEvent> out) throws Exception {
        if (!ready(ctx)) {
            parked.add(e);
            parkedEvents.inc();
            ctx.timerService().registerProcessingTimeTimer(ctx.currentProcessingTime() + RETRY_MS);
            return;
        }
        releaseParked(ctx, out);
        decide(e, ctx, out);
    }

    @Override
    public void onTimer(long ts, OnTimerContext ctx, Collector<EnrichedEvent> out) throws Exception {
        if (ready(ctx)) {
            releaseParked(ctx, out);
        } else {
            ctx.timerService().registerProcessingTimeTimer(ts + RETRY_MS);
        }
    }

    private boolean ready(ReadOnlyContext ctx) throws Exception {
        if (catalogLoaded) return true;
        boolean hasAssets = ctx.getBroadcastState(ASSETS).immutableEntries().iterator().hasNext();
        boolean quiet = ctx.currentProcessingTime() - lastControlAt > WARMUP_QUIET_MS;
        catalogLoaded = hasAssets && quiet;
        return catalogLoaded;
    }

    private void releaseParked(ReadOnlyContext ctx, Collector<EnrichedEvent> out) throws Exception {
        Iterable<AiEvent> waiting = parked.get();
        if (waiting == null || !waiting.iterator().hasNext()) return;
        for (AiEvent p : waiting) decide(p, ctx, out);
        parked.clear();
    }

    private void decide(AiEvent e, ReadOnlyContext ctx, Collector<EnrichedEvent> out) throws Exception {
        ReadOnlyBroadcastState<String, String> aliases = ctx.getBroadcastState(ALIASES);
        String assetId = aliases.get(normalize(e.assetRef));
        AssetRisk risk = assetId == null ? null : ctx.getBroadcastState(ASSETS).get(assetId);
        boolean known = risk != null;
        if (!known) {
            risk = AssetRisk.unknown(e.assetRef);
            unknownAssets.inc();
            if (reportedUnknown.get(e.assetRef) == null) {
                reportedUnknown.put(e.assetRef, true);
                ctx.output(NEW_ASSETS, Json.newAsset(e));
            }
        }
        EnrichedEvent o = EnrichedEvent.from(e, risk, known);
        PolicyRule hit = firstMatch(policies(ctx), o);
        o.policyId = hit == null ? null : hit.policyId;
        o.action = hit == null ? "allow" : hit.action;
        o.processedAt = ctx.currentProcessingTime();
        o.latencyMs = e.tsSent == null ? null : o.processedAt - e.tsSent;
        out.collect(o);
    }

    private List<PolicyRule> policies(ReadOnlyContext ctx) throws Exception {
        if (sortedPolicies == null) {
            List<PolicyRule> list = new ArrayList<>();
            for (Map.Entry<String, PolicyRule> en : ctx.getBroadcastState(POLICIES).immutableEntries()) list.add(en.getValue());
            list.sort(Comparator.comparingInt((PolicyRule p) -> p.priority).thenComparing(p -> p.policyId));
            sortedPolicies = list;
        }
        return sortedPolicies;
    }

    static PolicyRule firstMatch(List<PolicyRule> sorted, EnrichedEvent e) {
        for (PolicyRule p : sorted) if (p.matches(e)) return p;
        return null;
    }

    /** Sensors report references in many spellings; compare case-insensitively without surrounding spaces. */
    static String normalize(String ref) {
        return ref == null ? "" : ref.trim().toLowerCase(Locale.ROOT);
    }
}
