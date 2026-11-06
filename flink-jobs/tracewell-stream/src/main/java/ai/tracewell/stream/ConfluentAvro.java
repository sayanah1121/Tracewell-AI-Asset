package ai.tracewell.stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Decodes Confluent wire-format Avro (magic byte 0 + 4-byte schema id + Avro body) using the writer schema
 * fetched from the Schema Registry by id, so producers can evolve the schema without redeploying this job.
 */
public class ConfluentAvro implements Serializable {
    private final String registryUrl;
    private transient Map<Integer, GenericDatumReader<GenericRecord>> readers;
    private transient HttpClient http;

    public ConfluentAvro(String registryUrl) {
        this.registryUrl = registryUrl;
    }

    public GenericRecord decode(byte[] value) throws IOException {
        if (readers == null) {
            readers = new ConcurrentHashMap<>();
            http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
        }
        ByteBuffer buf = ByteBuffer.wrap(value);
        if (buf.get() != 0) throw new IOException("not Confluent wire format (bad magic byte)");
        int schemaId = buf.getInt();
        GenericDatumReader<GenericRecord> reader = readers.computeIfAbsent(schemaId, this::fetchReader);
        return reader.read(null, DecoderFactory.get().binaryDecoder(value, 5, value.length - 5, null));
    }

    private GenericDatumReader<GenericRecord> fetchReader(int id) {
        try {
            HttpResponse<String> resp = http.send(
                    HttpRequest.newBuilder(URI.create(registryUrl + "/schemas/ids/" + id)).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) throw new IOException("schema " + id + ": HTTP " + resp.statusCode());
            String schemaJson = new ObjectMapper().readTree(resp.body()).get("schema").asText();
            return new GenericDatumReader<>(new Schema.Parser().parse(schemaJson));
        } catch (IOException | InterruptedException e) {
            throw new IllegalStateException("cannot fetch schema id " + id + " from " + registryUrl, e);
        }
    }

    static String str(GenericRecord r, String field) {
        Object v = r.get(field);
        return v == null ? null : v.toString();
    }

    static Long lng(GenericRecord r, String field) {
        Object v = r.get(field);
        return v == null ? null : ((Number) v).longValue();
    }
}
