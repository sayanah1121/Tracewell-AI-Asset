package ai.tracewell.stream.model;

/** One link of the tamper-evident audit chain (row of silver.audit_log): a warn or block verdict. */
public class AuditRecord {
    public Integer chainId;        // which of the parallel chains (by device)
    public Long seqNo;             // 1, 2, 3, ... within the chain, no gaps
    public String eventId;
    public Long tsEvent;           // epoch millis
    public String identity;
    public String deviceId;
    public String assetId;
    public String action;          // warn | block
    public String policyId;
    public String bandAtEvent;
    public String[] sensitiveTypes;
    public String prevHash;        // record_hash of the previous link (64 zeros for the first)
    public String recordHash;      // sha256(prev_hash + canonical record)
    public Long writtenAt;

    public AuditRecord() {}
}
