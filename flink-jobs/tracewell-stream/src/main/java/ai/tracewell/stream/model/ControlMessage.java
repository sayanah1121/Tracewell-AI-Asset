package ai.tracewell.stream.model;

/** Element of the broadcast control stream: a catalog or policy upsert / delete (tombstone). */
public class ControlMessage {
    public static final String ASSET = "asset";
    public static final String POLICY = "policy";

    public String kind;           // asset | policy
    public String key;            // asset_id or policy_id
    public boolean delete;        // tombstone
    public AssetRisk asset;
    public PolicyRule policy;

    public ControlMessage() {}

    public static ControlMessage of(String kind, String key, boolean delete) {
        ControlMessage m = new ControlMessage();
        m.kind = kind;
        m.key = key;
        m.delete = delete;
        return m;
    }
}
