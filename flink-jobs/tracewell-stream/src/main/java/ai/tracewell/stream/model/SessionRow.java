package ai.tracewell.stream.model;

/** One activity session of one person on one device (row of silver.fct_sessions). */
public class SessionRow {
    public String sessionId;
    public String identity;
    public String deviceId;
    public String department;
    public Long sessionStart;      // epoch millis of the first event
    public Long sessionEnd;        // epoch millis of the last event
    public Long durationS;
    public Long eventCount;
    public String[] sensorsSeen;
    public String[] eventTypes;
    public String[] assetsUsed;
    public Integer unknownAssets;
    public Double maxRiskScore;
    public String maxBand;
    public Long sensitiveEvents;
    public Long blocks;
    public Long warns;
    public Double mbOut;
    public String source;          // "stream" (Flink window) or "backfill" (Spark, after late events)
    public Long builtAt;

    public SessionRow() {}
}
