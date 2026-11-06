package ai.tracewell.stream.model;

/** A deduplicated, redacted event with the risk profile used for its verdict (row of silver.fct_ai_events). */
public class EnrichedEvent {
    public String eventId;
    public Long tsEvent;
    public Long tsSent;
    public String deviceId;
    public String os;
    public String identity;
    public String department;
    public String sensor;
    public String eventType;
    public String assetRef;
    public String assetId;
    public Boolean assetKnown;
    public String assetCategory;
    public Double riskScoreAtEvent;
    public String bandAtEvent;
    public Boolean sanctionedAtEvent;
    public Long catalogValidFrom;
    public String assetVersion;
    public String toolName;
    public String promptText;
    public String fileName;
    public String destHost;
    public Long bytesOut;
    public String[] sensitiveTypes = new String[0];
    public Integer sensitiveCount = 0;
    public String policyId;
    public String action;
    public Long latencyMs;
    public Long processedAt;

    public EnrichedEvent() {}

    public static EnrichedEvent from(AiEvent e, AssetRisk risk, boolean known) {
        EnrichedEvent o = new EnrichedEvent();
        o.eventId = e.eventId;
        o.tsEvent = e.tsEvent;
        o.tsSent = e.tsSent;
        o.deviceId = e.deviceId;
        o.os = e.os;
        o.identity = e.identity;
        o.department = e.department;
        o.sensor = e.sensor;
        o.eventType = e.eventType;
        o.assetRef = e.assetRef;
        o.assetId = risk.assetId;
        o.assetKnown = known;
        o.assetCategory = risk.category;
        o.riskScoreAtEvent = risk.riskScore;
        o.bandAtEvent = risk.band;
        o.sanctionedAtEvent = risk.sanctioned;
        o.catalogValidFrom = risk.validFrom;
        o.assetVersion = e.assetVersion;
        o.toolName = e.toolName;
        o.promptText = e.promptText;
        o.fileName = e.fileName;
        o.destHost = e.destHost;
        o.bytesOut = e.bytesOut;
        o.sensitiveTypes = e.sensitiveTypes;
        o.sensitiveCount = e.sensitiveCount;
        return o;
    }
}
