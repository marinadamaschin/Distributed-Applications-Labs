package md.utm.messaging.contracts;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.*;
import java.nio.charset.StandardCharsets;

public final class JsonLineProtocol {
    private static final ObjectMapper MAPPER = new ObjectMapper().registerModule(new JavaTimeModule()).disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS).configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private JsonLineProtocol() {
    }

    public static ObjectMapper mapper() {
        return MAPPER;
    }

    public static void write(BufferedWriter w, TransportFrame f) throws IOException {
        w.write(MAPPER.writeValueAsString(f));
        w.newLine();
        w.flush();
    }

    public static TransportFrame read(BufferedReader r) throws IOException {
        String line = r.readLine();
        if (line == null) throw new EOFException("Connection closed before a frame was received");
        return MAPPER.readValue(line, TransportFrame.class);
    }

    public static BufferedReader reader(InputStream in) {
        return new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
    }

    public static BufferedWriter writer(OutputStream out) {
        return new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8));
    }
}
