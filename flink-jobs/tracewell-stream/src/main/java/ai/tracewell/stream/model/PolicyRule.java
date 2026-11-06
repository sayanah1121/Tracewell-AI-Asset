package ai.tracewell.stream.model;

import java.util.Arrays;
import java.util.Locale;

/**
 * One policy from policies/*.yaml. Every condition that is set must hold; unset conditions are ignored.
 * Arrays (not Sets) keep this a Flink POJO, so broadcast state needs no Kryo.
 */
public class PolicyRule {
    public String policyId;
    public String name;
    public int priority;
    public boolean enabled = true;
    public String action;                 // allow | warn | block

    public String[] sensitiveTypesAny;
    public Boolean assetSanctioned;
    public String[] assetBandIn;
    public Boolean assetKnown;
    public String[] eventTypeIn;
    public String[] toolNameIn;
    public String[] fileExtIn;

    public PolicyRule() {}

    public boolean matches(EnrichedEvent e) {
        if (!enabled) return false;
        if (sensitiveTypesAny != null && (e.sensitiveTypes == null
                || Arrays.stream(e.sensitiveTypes).noneMatch(t -> contains(sensitiveTypesAny, t)))) return false;
        if (assetSanctioned != null && !assetSanctioned.equals(e.sanctionedAtEvent)) return false;
        if (assetBandIn != null && !contains(assetBandIn, e.bandAtEvent)) return false;
        if (assetKnown != null && !assetKnown.equals(e.assetKnown)) return false;
        if (eventTypeIn != null && !contains(eventTypeIn, e.eventType)) return false;
        if (toolNameIn != null && !contains(toolNameIn, e.toolName)) return false;
        if (fileExtIn != null && !contains(fileExtIn, extension(e.fileName))) return false;
        return true;
    }

    static boolean contains(String[] values, String v) {
        if (v == null) return false;
        for (String x : values) if (x.equalsIgnoreCase(v)) return true;
        return false;
    }

    static String extension(String fileName) {
        if (fileName == null) return null;
        int dot = fileName.lastIndexOf('.');
        return dot < 0 ? null : fileName.substring(dot + 1).toLowerCase(Locale.ROOT);
    }
}
