package ai.tracewell.stream;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RedactorTest {

    @Test
    void cardNumbersWithSeparatorsAreMasked() {
        Redactor.Result r = Redactor.redact("the corporate card is 4111 1111 1111 1111 exp 09/29");
        assertEquals("the corporate card is [REDACTED:PCI] exp 09/29", r.text());
        assertArrayEquals(new String[]{"PCI"}, r.types());
        assertEquals(1, r.count());
        assertTrue(Redactor.redact("5555-5555-5555-4444").text().contains("[REDACTED:PCI]"));
        assertTrue(Redactor.redact("amex 378282246310005").text().contains("[REDACTED:PCI]"));
    }

    @Test
    void digitRunsThatFailLuhnAreNotCards() {
        Redactor.Result r = Redactor.redact("order 1234 5678 9012 3456 shipped");
        assertFalse(r.text().contains("PCI"));
    }

    @Test
    void secretsAreMasked() {
        assertEquals("use key [REDACTED:SECRET]", Redactor.redact("use key AKIAABCDEFGHIJKLMNOP").text());
        String openai = "my key is sk-" + "a".repeat(40);
        assertEquals("my key is [REDACTED:SECRET]", Redactor.redact(openai).text());
        assertTrue(Redactor.redact("token ghp_" + "x".repeat(36)).text().contains("[REDACTED:SECRET]"));
    }

    @Test
    void personalDataAndGovernmentIdsAreMasked() {
        Redactor.Result r = Redactor.redact("Contact Jane at jane.doe@example.com or +91 98765 43210, PAN ABCDE1234F, Aadhaar 2345 6789 0123");
        assertEquals("Contact Jane at [REDACTED:PII] or [REDACTED:PII], PAN [REDACTED:GOV_ID], Aadhaar [REDACTED:GOV_ID]", r.text());
        assertArrayEquals(new String[]{"GOV_ID", "PII"}, r.types());
        assertEquals(4, r.count());
    }

    @Test
    void ordinaryTextIsUntouched() {
        String s = "Generate SQL to find customers who churned last quarter (Q3 2026, 120 rows)";
        Redactor.Result r = Redactor.redact(s);
        assertEquals(s, r.text());
        assertEquals(0, r.count());
        assertEquals(0, Redactor.redact(null).count());
    }

    @Test
    void luhn() {
        assertTrue(Redactor.isCardNumber("4012888888881881"));
        assertFalse(Redactor.isCardNumber("4012888888881882"));
        assertFalse(Redactor.isCardNumber("123456789012"));
    }
}
