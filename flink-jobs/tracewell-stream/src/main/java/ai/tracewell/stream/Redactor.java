package ai.tracewell.stream;

import java.io.Serializable;
import java.util.List;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Masks sensitive values in free text before anything is stored. Detectors run in order (secrets and
 * card numbers before phone numbers, so a card is never half-masked as a phone). Only the categories and
 * the number of matches are kept: never the values.
 */
public final class Redactor implements Serializable {

    public record Result(String text, String[] types, int count) {}

    private record Detector(String type, Pattern pattern, Predicate<String> accept) implements Serializable {}

    private static final List<Detector> DETECTORS = List.of(
            new Detector("SECRET", Pattern.compile("\\bAKIA[0-9A-Z]{16}\\b"), m -> true),
            new Detector("SECRET", Pattern.compile("\\bsk-[A-Za-z0-9_-]{20,}"), m -> true),
            new Detector("SECRET", Pattern.compile("\\bgh[pousr]_[A-Za-z0-9]{30,}\\b"), m -> true),
            new Detector("SECRET", Pattern.compile("\\bxox[abprs]-[A-Za-z0-9-]{10,}\\b"), m -> true),
            new Detector("PCI", Pattern.compile("\\b(?:\\d[ -]?){12,18}\\d\\b"), Redactor::isCardNumber),
            new Detector("GOV_ID", Pattern.compile("\\b[A-Z]{5}\\d{4}[A-Z]\\b"), m -> true),                 // Indian PAN
            new Detector("GOV_ID", Pattern.compile("\\b[2-9]\\d{3}[ -]?\\d{4}[ -]?\\d{4}\\b"), m -> true),   // Aadhaar
            new Detector("PII", Pattern.compile("\\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}\\b"), m -> true),
            new Detector("PII", Pattern.compile("(?:\\+\\d{1,3}[ -]?)?\\b\\d{5}[ -]?\\d{5}\\b"), m -> true)    // phone
    );

    private Redactor() {}

    public static Result redact(String text) {
        if (text == null || text.isEmpty()) return new Result(text, new String[0], 0);
        TreeSet<String> types = new TreeSet<>();
        int count = 0;
        String out = text;
        for (Detector d : DETECTORS) {
            Matcher m = d.pattern().matcher(out);
            StringBuilder sb = new StringBuilder();
            boolean changed = false;
            while (m.find()) {
                if (d.accept().test(m.group())) {
                    m.appendReplacement(sb, Matcher.quoteReplacement("[REDACTED:" + d.type() + "]"));
                    types.add(d.type());
                    count++;
                    changed = true;
                }
            }
            if (changed) {
                m.appendTail(sb);
                out = sb.toString();
            }
        }
        return new Result(out, types.toArray(new String[0]), count);
    }

    /** 13-19 digits that pass the Luhn checksum. */
    static boolean isCardNumber(String candidate) {
        String digits = candidate.replaceAll("[^0-9]", "");
        if (digits.length() < 13 || digits.length() > 19) return false;
        int sum = 0;
        boolean alt = false;
        for (int i = digits.length() - 1; i >= 0; i--) {
            int n = digits.charAt(i) - '0';
            if (alt) {
                n *= 2;
                if (n > 9) n -= 9;
            }
            sum += n;
            alt = !alt;
        }
        return sum % 10 == 0;
    }
}
