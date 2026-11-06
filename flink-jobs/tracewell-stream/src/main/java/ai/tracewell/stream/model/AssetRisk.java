package ai.tracewell.stream.model;

/** Current risk profile of one asset, as published by the Spark catalog to `asset-risk-updates`. */
public class AssetRisk {
    public String assetId;
    public String assetName;
    public String category;
    public Double riskScore;
    public String band;
    public Boolean sanctioned;
    public String[] aliases = new String[0];
    public Long validFrom;        // epoch millis of the SCD2 version

    public AssetRisk() {}

    /** Provisional profile for an asset the catalog has never seen: treated as High until profiled. */
    public static AssetRisk unknown(String ref) {
        AssetRisk a = new AssetRisk();
        a.assetId = ref;
        a.assetName = ref;
        a.category = "unknown";
        a.riskScore = 7.5;
        a.band = "High";
        a.sanctioned = false;
        return a;
    }
}
