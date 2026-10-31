package md.utm.pad;

import java.io.*;
import java.net.URI;
import java.net.http.*;
import java.time.Duration;
import java.util.*;

public final class RemoteEmployeeStore implements EmployeeStore {
    private final URI endpoint;
    private final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(1)).build();
    public RemoteEmployeeStore(String base) { endpoint = URI.create(base + "/store"); }
    private interface Writer { void write(DataOutputStream out) throws IOException; }
    private DataInputStream call(String op, Writer writer) throws IOException {
        var bytes = new ByteArrayOutputStream();
        try (var out = new DataOutputStream(bytes)) { out.writeUTF(op); writer.write(out); }
        try {
            var request = HttpRequest.newBuilder(endpoint).timeout(Duration.ofSeconds(3))
                .header("Content-Type", "application/octet-stream").POST(HttpRequest.BodyPublishers.ofByteArray(bytes.toByteArray())).build();
            var result = client.send(request, HttpResponse.BodyHandlers.ofByteArray());
            if (result.statusCode() != 200) throw new IOException("Storage status=" + result.statusCode());
            return new DataInputStream(new ByteArrayInputStream(result.body()));
        } catch (InterruptedException ex) { Thread.currentThread().interrupt(); throw new IOException("Interrupted storage request", ex); }
    }
    public List<Employee> list(int offset, int limit) throws IOException {
        try (var in = call("list", out -> { out.writeInt(offset); out.writeInt(limit); })) {
            var rows = new ArrayList<Employee>(); int count = in.readInt();
            for (int i = 0; i < count; i++) rows.add(PersistentEmployeeStore.readEmployee(in)); return List.copyOf(rows);
        }
    }
    public Employee get(int id) throws IOException {
        try (var in = call("get", out -> out.writeInt(id))) { return in.readBoolean() ? PersistentEmployeeStore.readEmployee(in) : null; }
    }
    public boolean put(Employee e) throws IOException {
        try (var in = call("put-v1", out -> PersistentEmployeeStore.writeEmployee(out, e))) { return in.readBoolean(); }
    }
    public long propose(Employee e) throws IOException {
        try (var in = call("propose-v1", out -> PersistentEmployeeStore.writeEmployee(out, e))) { return in.readLong(); }
    }
    public List<Change> changes(long after) throws IOException {
        try (var in = call("changes", out -> out.writeLong(after))) {
            var rows = new ArrayList<Change>(); int count = in.readInt();
            for (int i = 0; i < count; i++) rows.add(new Change(in.readLong(), PersistentEmployeeStore.readEmployee(in))); return List.copyOf(rows);
        }
    }
    public void health() throws IOException { try (var ignored = call("health", out -> {})) {} }
}
