package md.utm.pad;

import com.sun.net.httpserver.*;
import java.io.IOException;
import java.net.URI;
import java.net.http.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public final class ProxyController implements HttpHandler, AutoCloseable {
    private record Cached(byte[] body, String type, String node, long expires, long generation) {}
    private static final class Node {
        final String url;
        volatile boolean healthy;
        Node(String url) { this.url = url; }
    }
    private static final class Session {
        int requests;
        long touched = System.nanoTime();
    }
    private final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(1))
        .version(HttpClient.Version.HTTP_1_1).build();
    private final List<Node> nodes;
    private final AtomicInteger next = new AtomicInteger();
    private final ScheduledExecutorService healthChecks = Executors.newSingleThreadScheduledExecutor();
    private final Map<String, Cached> cache = new HashMap<>();

    private final Object cacheState = new Object();
    private long generation;
    private int activeWrites;
    private final Map<String, Session> sessions = new HashMap<>();
    private final long ttlNanos;

    public ProxyController(List<String> endpoints, long ttlMillis, long healthMillis) {
        if (endpoints.isEmpty() || ttlMillis <= 0 || healthMillis <= 0) throw new IllegalArgumentException("Invalid proxy configuration");
        nodes = endpoints.stream().distinct().map(Node::new).toList();
        ttlNanos = TimeUnit.MILLISECONDS.toNanos(ttlMillis);
        checkHealth();
        healthChecks.scheduleWithFixedDelay(this::checkHealth, healthMillis, healthMillis, TimeUnit.MILLISECONDS);
    }
    private void checkHealth() {
        for (Node node : nodes) {
            boolean healthy;
            try {
                var request = HttpRequest.newBuilder(URI.create(node.url + "/health"))
                    .timeout(Duration.ofMillis(700)).header("X-Correlation-Id", "health-" + UUID.randomUUID()).GET().build();
                healthy = client.send(request, HttpResponse.BodyHandlers.discarding()).statusCode() == 200;
            } catch (InterruptedException ex) { Thread.currentThread().interrupt(); return; }
            catch (Exception ex) { healthy = false; }
            if (node.healthy != healthy) System.out.printf("HEALTH %s %s%n", node.url, healthy ? "UP" : "DOWN");
            node.healthy = healthy;
        }
    }

    private void session(HttpExchange e) {
        String id = null;
        String cookie = e.getRequestHeaders().getFirst("Cookie");
        if (cookie != null) for (String item : cookie.split(";")) {
            String[] pair = item.trim().split("=", 2);
            if (pair.length == 2 && pair[0].equals("PADSESSION")) id = pair[1];
        }
        int count;
        synchronized (sessions) {
            long now = System.nanoTime();
            sessions.entrySet().removeIf(entry -> now - entry.getValue().touched > TimeUnit.MINUTES.toNanos(30));
            if (id == null || !sessions.containsKey(id)) {
                if (sessions.size() >= 1000) {
                    String oldest = sessions.entrySet().stream().min(Comparator.comparingLong(x -> x.getValue().touched)).orElseThrow().getKey();
                    sessions.remove(oldest);
                }
                id = UUID.randomUUID().toString(); sessions.put(id, new Session());
            }
            Session state = sessions.get(id); state.touched = now; count = ++state.requests;
        }
        e.getResponseHeaders().add("Set-Cookie", "PADSESSION=" + id + "; Path=/; HttpOnly; SameSite=Lax; Max-Age=1800");
        e.getResponseHeaders().set("X-Session-Id", id);
        e.getResponseHeaders().set("X-Session-Requests", Integer.toString(count));
    }
    public void handle(HttpExchange e) throws IOException {
        String correlation = HttpUtil.correlation(e);
        e.getResponseHeaders().set("X-Correlation-Id", correlation);
        session(e);
        try { dispatch(e, correlation); }
        finally {
            System.out.printf("PROXY correlation=%s session=%s request=%s method=%s uri=%s status=%d cache=%s warehouse=%s%n",
                correlation, e.getResponseHeaders().getFirst("X-Session-Id"), e.getResponseHeaders().getFirst("X-Session-Requests"),
                e.getRequestMethod(), e.getRequestURI(), e.getResponseCode(),
                e.getResponseHeaders().getFirst("X-Cache"), e.getResponseHeaders().getFirst("X-Warehouse"));
        }
    }
    private void dispatch(HttpExchange e, String correlation) throws IOException {
        String path = e.getRequestURI().getPath(), method = e.getRequestMethod();
        if (!Set.of("/employees", "/updates/employees", "/health").contains(path)) { HttpUtil.send(e, 404, "text/plain", "Not found"); return; }
        if (!Set.of("GET", "PUT", "POST").contains(method) || (!path.equals("/employees") && !method.equals("GET"))) {
            e.getResponseHeaders().set("Allow", path.equals("/employees") ? "GET, PUT, POST" : "GET");
            HttpUtil.send(e, 405, "text/plain", "Method not allowed"); return;
        }
        if (path.equals("/health")) {
            boolean available = nodes.stream().anyMatch(n -> n.healthy);
            HttpUtil.send(e, available ? 200 : 503, "text/plain", available ? "proxy ready" : "No healthy warehouse"); return;
        }
        String accept = HttpUtil.negotiate(e.getRequestHeaders().getFirst("Accept"));
        if (accept == null) { HttpUtil.send(e, 406, "text/plain", "Supported formats: application/json, application/xml"); return; }
        byte[] body = e.getRequestBody().readNBytes(8193);
        if (body.length > 8192) { HttpUtil.send(e, 413, "text/plain", "Body too large"); return; }
        boolean read = method.equals("GET");
        String directive = Optional.ofNullable(e.getRequestHeaders().getFirst("Cache-Control")).orElse("").toLowerCase(Locale.ROOT);
        boolean eligible = read && path.equals("/employees") && !directive.contains("no-cache") && !directive.contains("no-store");
        String key = e.getRequestURI().toASCIIString() + "|" + accept;
        long startGeneration;
        Cached hit = null;
        synchronized (cacheState) {
            if (!read) { activeWrites++; generation++; cache.clear(); }
            startGeneration = generation;
            if (eligible && activeWrites == 0) {
                Cached candidate = cache.get(key);
                if (candidate != null && candidate.expires() > System.nanoTime() && candidate.generation() == generation) hit = candidate;
            }
        }
        try {
            if (hit != null) { send(e, 200, hit, "HIT"); return; }
            List<Node> available = nodes.stream().filter(n -> n.healthy).toList();
            if (available.isEmpty()) { HttpUtil.send(e, 503, "text/plain", "No healthy warehouse"); return; }
            int index = Math.floorMod(next.getAndIncrement(), available.size());
            HttpResponse<byte[]> response = null;
            int attempts = read ? available.size() : 1;
            for (int attempt = 0; attempt < attempts; attempt++) {
                Node node = available.get((index + attempt) % available.size());
                var builder = HttpRequest.newBuilder(URI.create(node.url + e.getRequestURI().toASCIIString()))
                    .timeout(Duration.ofSeconds(5)).header("Accept", accept).header("X-Correlation-Id", correlation);
                String contentType = e.getRequestHeaders().getFirst("Content-Type");
                if (contentType != null) builder.header("Content-Type", contentType);
                builder.method(method, HttpRequest.BodyPublishers.ofByteArray(body));
                try {
                    response = client.send(builder.build(), HttpResponse.BodyHandlers.ofByteArray());
                    if (response.statusCode() >= 500) {
                        node.healthy = false;
                        if (read && attempt + 1 < attempts) continue;
                    }
                    break;
                } catch (IOException ex) {
                    node.healthy = false;
                    if (attempt + 1 == attempts) { HttpUtil.send(e, 502, "text/plain", "Warehouse unavailable; write outcome may be unknown"); return; }
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt(); HttpUtil.send(e, 503, "text/plain", "Interrupted"); return;
                }
            }
            Cached result = new Cached(response.body(), response.headers().firstValue("Content-Type").orElse("application/octet-stream"),
                response.headers().firstValue("X-Warehouse").orElse("unknown"), System.nanoTime() + ttlNanos, startGeneration);
            if (eligible && response.statusCode() == 200) synchronized (cacheState) {
                // Checking and insertion are atomic with respect to beginning/ending writes.
                if (activeWrites == 0 && generation == startGeneration) {
                    if (cache.size() >= 1000) cache.clear();
                    cache.put(key, result);
                }
            }
            response.headers().firstValue("Location").ifPresent(v -> e.getResponseHeaders().set("Location", v));
            send(e, response.statusCode(), result, eligible ? "MISS" : "BYPASS");
        } finally {
            if (!read) synchronized (cacheState) {
                activeWrites--; generation++; cache.clear();
            }
        }
    }
    private void send(HttpExchange e, int status, Cached c, String cacheStatus) throws IOException {
        e.getResponseHeaders().set("Content-Type", c.type());
        e.getResponseHeaders().set("X-Warehouse", c.node());
        e.getResponseHeaders().set("X-Cache", cacheStatus);
        e.getResponseHeaders().set("Cache-Control", "no-store");
        e.getResponseHeaders().set("Vary", "Accept");
        e.sendResponseHeaders(status, c.body().length); e.getResponseBody().write(c.body());
    }
    public void close() { healthChecks.shutdownNow(); }
}
