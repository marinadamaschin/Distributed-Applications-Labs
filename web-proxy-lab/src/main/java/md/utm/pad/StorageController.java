package md.utm.pad;

import com.sun.net.httpserver.*;
import java.io.*;

public final class StorageController implements HttpHandler {
    private final EmployeeStore store;
    public StorageController(EmployeeStore store) { this.store = store; }
    public void handle(HttpExchange e) throws IOException {
        if (e.getRequestURI().getPath().equals("/health") && e.getRequestMethod().equals("GET")) {
            store.health(); HttpUtil.send(e, 200, "text/plain", "storage OK"); return;
        }
        if (!e.getRequestURI().getPath().equals("/store")) { HttpUtil.send(e, 404, "text/plain", "Not found"); return; }
        if (!e.getRequestMethod().equals("POST")) { HttpUtil.send(e, 405, "text/plain", "POST required"); return; }
        byte[] request = e.getRequestBody().readNBytes(8193);
        if (request.length > 8192) { HttpUtil.send(e, 413, "text/plain", "Too large"); return; }
        var bytes = new ByteArrayOutputStream();
        try (var in = new DataInputStream(new ByteArrayInputStream(request)); var out = new DataOutputStream(bytes)) {
            switch (in.readUTF()) {
                case "list" -> {
                    int offset = in.readInt(), limit = in.readInt();
                    if (offset < 0 || limit < 1 || limit > 1000) throw new IllegalArgumentException("Invalid pagination");
                    var rows = store.list(offset, limit); out.writeInt(rows.size());
                    for (var row : rows) PersistentEmployeeStore.writeEmployee(out, row);
                }
                case "get" -> { var row = store.get(in.readInt()); out.writeBoolean(row != null); if (row != null) PersistentEmployeeStore.writeEmployee(out, row); }
                case "put", "propose" -> throw new IllegalArgumentException("Use versioned write operation");
                case "put-v1" -> out.writeBoolean(store.put(validEmployee(in)));
                case "propose-v1" -> out.writeLong(store.propose(validEmployee(in)));
                case "changes" -> {
                    long after = in.readLong(); if (after < 0) throw new IllegalArgumentException("after >= 0");
                    var changes = store.changes(after); out.writeInt(changes.size());
                    for (var c : changes) { out.writeLong(c.sequence()); PersistentEmployeeStore.writeEmployee(out, c.employee()); }
                }
                case "health" -> store.health();
                default -> throw new IllegalArgumentException("Unknown operation");
            }
        }
        byte[] body = bytes.toByteArray();
        e.getResponseHeaders().set("Content-Type", "application/octet-stream");
        e.sendResponseHeaders(200, body.length == 0 ? -1 : body.length);
        if (body.length > 0) e.getResponseBody().write(body);
    }
    private static EmployeeStore.Employee validEmployee(DataInputStream in) throws IOException {
        var row = PersistentEmployeeStore.readEmployee(in);
        if (row.id() <= 0 || row.name().isBlank() || row.name().length() > 100 || row.name().chars().anyMatch(c -> c < 32))
            throw new IllegalArgumentException("Invalid employee");
        return row;
    }
}
