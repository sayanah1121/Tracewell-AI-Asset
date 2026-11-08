package ai.tracewell.stream;

import ai.tracewell.stream.model.EnrichedEvent;
import ai.tracewell.stream.model.SessionRow;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class SessionsTest {

    static final long T0 = 1_793_005_200_000L;   // 2026-10-26 09:00 UTC

    static EnrichedEvent ev(long ts, String asset, String band, double score, String action) {
        EnrichedEvent e = new EnrichedEvent();
        e.tsEvent = ts;
        e.deviceId = "WX-1";
        e.identity = "ann@corp.example";
        e.department = "finance";
        e.sensor = "browser";
        e.eventType = "prompt";
        e.assetId = asset;
        e.assetKnown = !asset.startsWith("mcp:unknown");
        e.bandAtEvent = band;
        e.riskScoreAtEvent = score;
        e.action = action;
        return e;
    }

    @Test
    void sessionIdMatchesTheSparkFormula() {
        // sha256("WX-1|ann@corp.example|1793005200000")[:16], also asserted in tests/sessions (Spark)
        assertEquals("4df2cccb30098f73", Sessions.sessionId("WX-1", "ann@corp.example", T0));
    }

    @Test
    void mergingTwoPartialSessionsEqualsOneSession() {
        Sessions.Agg agg = new Sessions.Agg();
        EnrichedEvent[] events = {
                ev(T0, "service:chatgpt", "Low", 4.4, "allow"),
                ev(T0 + 60_000, "mcp:unknown-x", "High", 7.0, "warn"),
                ev(T0 + 120_000, "agent:cursor", "Medium", 5.8, "block"),
        };
        Sessions.Acc whole = agg.createAccumulator();
        for (EnrichedEvent e : events) whole = agg.add(e, whole);
        // a late event fills the gap between two windows: Flink merges their accumulators
        Sessions.Acc left = agg.add(events[0], agg.createAccumulator());
        Sessions.Acc right = agg.add(events[2], agg.add(events[1], agg.createAccumulator()));
        Sessions.Acc merged = agg.merge(left, right);

        SessionRow a = Sessions.toRow(whole, "stream", 0), b = Sessions.toRow(merged, "stream", 0);
        for (SessionRow r : new SessionRow[]{a, b}) {
            assertEquals(Sessions.sessionId("WX-1", "ann@corp.example", T0), r.sessionId);
            assertEquals(T0, r.sessionStart);
            assertEquals(T0 + 120_000, r.sessionEnd);
            assertEquals(120L, r.durationS);
            assertEquals(3L, r.eventCount);
            assertArrayEquals(new String[]{"agent:cursor", "mcp:unknown-x", "service:chatgpt"}, r.assetsUsed);
            assertEquals(1, r.unknownAssets);
            assertEquals(7.0, r.maxRiskScore);
            assertEquals("High", r.maxBand);
            assertEquals(1L, r.blocks);
            assertEquals(1L, r.warns);
        }
    }
}
