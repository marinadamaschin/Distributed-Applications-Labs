package md.utm.pad;

import com.sun.net.httpserver.*;
import java.net.*;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Main {
    public static void main(String[] args) throws Exception {
        EmployeeStore store = new EmployeeStore();
        List<HttpServer> servers = new ArrayList<>();
        List<ExecutorService> pools = new ArrayList<>();
        try {
            servers.add(start(8081, new WarehouseController(store, "dw-1"), pools));
            servers.add(start(8082, new WarehouseController(store, "dw-2"), pools));
            servers.add(start(8080, new ProxyController(), pools));
        } catch (Exception e) {
            servers.forEach(s -> s.stop(0)); pools.forEach(ExecutorService::shutdownNow); throw e;
        }
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            servers.forEach(s -> s.stop(0)); pools.forEach(ExecutorService::shutdownNow);
        }));
        System.out.println("Proxy http://localhost:8080/employees | DW: 8081, 8082 | Ctrl+C to stop");
    }
    static HttpServer start(int port, HttpHandler handler, List<ExecutorService> pools) throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 64);
        ExecutorService pool = Executors.newFixedThreadPool(32);
        pools.add(pool); server.setExecutor(pool);
        server.createContext("/", exchange -> {
            try { handler.handle(exchange); }
            catch (IllegalArgumentException e) { HttpUtil.send(exchange, 400, "text/plain", e.getMessage()); }
            catch (Exception e) { e.printStackTrace(); HttpUtil.send(exchange, 500, "text/plain", "Internal error"); }
            finally { exchange.close(); }
        });
        server.start(); return server;
    }
}
