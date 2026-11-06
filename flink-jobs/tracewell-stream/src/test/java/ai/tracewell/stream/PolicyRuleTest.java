package ai.tracewell.stream;

import ai.tracewell.stream.model.EnrichedEvent;
import ai.tracewell.stream.model.PolicyRule;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class PolicyRuleTest {

    // Same rules as policies/default.yaml, in the JSON form publish_policies.py sends to Kafka.
    private static final String RULES = """
            [
             {"policy_id":"P001","name":"Block payment data to unsanctioned AI","priority":10,"action":"block",
              "when":{"sensitive_types_any":["PCI"],"asset_sanctioned":false}},
             {"policy_id":"P002","name":"Block critical-risk assets","priority":20,"action":"block",
              "when":{"asset_band_in":["Critical"]}},
             {"policy_id":"P003","name":"Block secrets sent to unsanctioned AI","priority":30,"action":"block",
              "when":{"sensitive_types_any":["SECRET"],"asset_sanctioned":false}},
             {"policy_id":"P004","name":"Warn on any sensitive data sent to AI","priority":40,"action":"warn",
              "when":{"sensitive_types_any":["PCI","PII","SECRET","GOV_ID"]}},
             {"policy_id":"P005","name":"Warn on high-risk MCP tools touching data","priority":50,"action":"warn",
              "when":{"event_type_in":["mcp_tool_call"],"asset_band_in":["High","Critical"],"tool_name_in":["read_file","query"]}},
             {"policy_id":"P006","name":"Warn on spreadsheets uploaded to unsanctioned AI","priority":60,"action":"warn",
              "when":{"event_type_in":["file_upload"],"asset_sanctioned":false,"file_ext_in":["xlsx","csv"]}},
             {"policy_id":"P007","name":"Warn on unknown AI assets","priority":70,"action":"warn",
              "when":{"asset_known":false}}
            ]""";

    private static List<PolicyRule> rules() throws Exception {
        List<PolicyRule> out = new ArrayList<>();
        new ObjectMapper().readTree(RULES).forEach(n -> out.add(Deserializers.Control.parsePolicyJson(n)));
        out.sort(Comparator.comparingInt(p -> p.priority));
        return out;
    }

    private static EnrichedEvent event(String type, String band, boolean sanctioned, String... sensitive) {
        EnrichedEvent e = new EnrichedEvent();
        e.eventType = type;
        e.bandAtEvent = band;
        e.sanctionedAtEvent = sanctioned;
        e.assetKnown = true;
        e.sensitiveTypes = sensitive;
        return e;
    }

    private static String verdict(EnrichedEvent e) throws Exception {
        PolicyRule hit = EnrichAndDecide.firstMatch(rules(), e);
        return hit == null ? "allow" : hit.policyId + ":" + hit.action;
    }

    @Test
    void cardDataToUnsanctionedAiIsBlocked() throws Exception {
        assertEquals("P001:block", verdict(event("prompt", "Low", false, "PCI")));
    }

    @Test
    void cardDataToSanctionedAiIsOnlyWarned() throws Exception {
        assertEquals("P004:warn", verdict(event("prompt", "Medium", true, "PCI")));
    }

    @Test
    void criticalAssetIsBlockedEvenWithoutSensitiveData() throws Exception {
        assertEquals("P002:block", verdict(event("asset_discovered", "Critical", false)));
    }

    @Test
    void secretToUnsanctionedAgentIsBlocked() throws Exception {
        assertEquals("P003:block", verdict(event("paste", "High", false, "SECRET")));
    }

    @Test
    void spreadsheetUploadAndUnknownAssetsAreWarned() throws Exception {
        EnrichedEvent upload = event("file_upload", "Medium", false);
        upload.fileName = "finance_q3.XLSX";
        assertEquals("P006:warn", verdict(upload));
        EnrichedEvent unknown = event("mcp_tool_call", "High", false);
        unknown.assetKnown = false;
        unknown.toolName = "search";
        assertEquals("P007:warn", verdict(unknown));
    }

    @Test
    void ordinaryUseIsAllowed() throws Exception {
        assertEquals("allow", verdict(event("prompt", "Medium", true)));
        EnrichedEvent e = event("mcp_tool_call", "Low", false);
        e.toolName = "read_file";
        assertEquals("allow", verdict(e));
    }

    @Test
    void disabledRulesNeverMatch() throws Exception {
        List<PolicyRule> r = rules();
        r.forEach(p -> p.enabled = false);
        assertNull(EnrichAndDecide.firstMatch(r, event("prompt", "Critical", false, "PCI")));
    }
}
