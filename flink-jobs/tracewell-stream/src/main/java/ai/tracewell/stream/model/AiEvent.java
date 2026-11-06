package ai.tracewell.stream.model;

/** One AI action from one device (Flink POJO: public fields, no-arg constructor). */
public class AiEvent {
    public String eventId;
    public Long tsEvent;          // epoch millis, event time on the device
    public Long tsSent;           // epoch millis, when the device sent it
    public String deviceId;
    public String os;
    public String identity;
    public String department;
    public String sensor;
    public String eventType;
    public String assetRef;
    public String assetVersion;
    public String toolName;
    public String promptText;     // raw until Redactor runs, redacted afterwards
    public String fileName;
    public Long fileSizeBytes;
    public String destHost;
    public Long bytesOut;
    public Integer schemaVersion;

    // set by Redactor
    public String[] sensitiveTypes = new String[0];
    public Integer sensitiveCount = 0;

    // set when the record could not be decoded (routed to the DLQ)
    public String parseError;
    public String rawBase64;

    public AiEvent() {}
}
