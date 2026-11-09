package ai.tracewell.stream;

import ai.tracewell.stream.model.AuditRecord;
import ai.tracewell.stream.model.EnrichedEvent;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AuditChainTest {

    static EnrichedEvent block() {
        EnrichedEvent e = new EnrichedEvent();
        e.eventId = "e1";
        e.tsEvent = 1_793_005_200_000L;   // 2026-10-26 09:00 UTC
        e.identity = "ann@corp.example";
        e.deviceId = "WX-1";
        e.assetId = "service:grok";
        e.action = "block";
        e.policyId = "P001";
        e.bandAtEvent = "Low";
        e.sensitiveTypes = new String[]{"PCI", "PII"};
        return e;
    }

    @Test
    void canonicalRecordAndHashMatchTheSparkVerifier() {
        AuditRecord r = AuditChain.link(block(), 2, 1, AuditChain.GENESIS);
        assertEquals("1|2|e1|1793005200000|ann@corp.example|WX-1|service:grok|block|P001|Low|PCI,PII",
                AuditChain.canonical(r));
        // same vector as tests/audit/test_verify_audit_chain.py (Spark)
        assertEquals("06a14ad7f39277269df4fcd726f6d4b8462d0bb4891e4ee6b1490200e7c6ed64", r.recordHash);
    }

    @Test
    void eachLinkCommitsToThePreviousOne() {
        AuditRecord first = AuditChain.link(block(), 0, 1, AuditChain.GENESIS);
        AuditRecord second = AuditChain.link(block(), 0, 2, first.recordHash);
        assertEquals(first.recordHash, second.prevHash);
        // same record, different predecessor -> different hash: a re-ordered or forged chain is visible
        assertNotEquals(second.recordHash, AuditChain.link(block(), 0, 2, AuditChain.GENESIS).recordHash);
    }

    @Test
    void chainsSpreadDevicesDeterministically() {
        assertEquals(AuditChain.chainOf("WX-1"), AuditChain.chainOf("WX-1"));
        for (String d : new String[]{"WX-1", "WX-2", "WX-T13728", ""}) {
            int c = AuditChain.chainOf(d);
            assertTrue(c >= 0 && c < AuditChain.CHAINS);
        }
    }
}
