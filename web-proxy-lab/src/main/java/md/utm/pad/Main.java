package md.utm.pad;

import com.sun.net.httpserver.*;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;

/** Exactly one role / HTTP server per JVM. */
public final class Main {
    public static void main(String[] args) throws Exception {
        if (args.length == 0) throw new IllegalArgumentException("Use: storage [8090] [data/store.bin] | dw [8081] [dw-1] | proxy [8080]");
        String role = args[0];
        int defaultPort = switch (role) { case "storage" -> 8090; case "dw" -> 8081; case "proxy" -> 8080; default -> throw new IllegalArgumentException("Unknown role: " + role); };
        int port = Integer.parseInt(arg(args, 1, "PORT", Integer.toString(defaultPort)));
        HttpHandler handler;
        AutoCloseable resource = () -> {};
        switch (role) {
            case "storage" -> {
                var store = new PersistentEmployeeStore(Path.of(arg(args, 2, "STORE_FILE", "data/store.bin")));
                handler = new StorageController(store); resource = store;
            }
            case "dw" -> handler = new WarehouseController(new RemoteEmployeeStore(env("STORE_URL", "http://127.0.0.1:8090")), arg(args, 2, "NODE_ID", "dw-" + port));
            case "proxy" -> {
                var proxy = new ProxyController(Arrays.stream(env("DW_ENDPOINTS", "http://127.0.0.1:8081,http://127.0.0.1:8082").split(",")).map(String::trim).toList(),
                    Long.parseLong(env("CACHE_TTL_MS", "5000")), Long.parseLong(env("HEALTH_INTERVAL_MS", "1000")));
                handler = proxy; resource = proxy;
            }
            default -> throw new IllegalArgumentException("Unknown role");
        }
        final AutoCloseable cleanup = resource;
        var pool = Executors.newFixedThreadPool(32);
        HttpServer server;
        try {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 64);
            server.setExecutor(pool);
            server.createContext("/", exchange -> {
                try { handler.handle(exchange); }
                catch (IllegalArgumentException ex) { if (exchange.getResponseCode() == -1) HttpUtil.send(exchange, 400, "text/plain", ex.getMessage()); }
                catch (Exception ex) {
                    ex.printStackTrace();
                    if (exchange.getResponseCode() == -1) HttpUtil.send(exchange, ex instanceof IOException ? 503 : 500, "text/plain", "Service error");
                } finally { exchange.close(); }
            });
            server.start();
        } catch (Exception ex) { pool.shutdownNow(); cleanup.close(); throw ex; }
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.stop(0); pool.shutdownNow();
            try { cleanup.close(); } catch (Exception ex) { ex.printStackTrace(); }
        }));
        System.out.printf("role=%s pid=%d http://127.0.0.1:%d%n", role, ProcessHandle.current().pid(), port);
    }
    private static String env(String name, String fallback) { return System.getenv().getOrDefault(name, fallback); }
    private static String arg(String[] args, int index, String env, String fallback) { return args.length > index ? args[index] : env(env, fallback); }
}
