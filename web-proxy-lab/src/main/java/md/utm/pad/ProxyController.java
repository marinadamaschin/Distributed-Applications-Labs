package md.utm.pad;
import com.sun.net.httpserver.*;
import java.io.IOException;
import java.net.URI;
import java.net.http.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public final class ProxyController implements HttpHandler {
    private record Cached(byte[] body,String type,String node,long expires) {}
    private final HttpClient client=HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).version(HttpClient.Version.HTTP_1_1).build();
    private final AtomicInteger next=new AtomicInteger();
    private final Map<String,Cached> cache=new ConcurrentHashMap<>();
    private final Map<String,String> inFlight=new ConcurrentHashMap<>();
    private final ReentrantReadWriteLock gate=new ReentrantReadWriteLock(true);
    public void handle(HttpExchange e) throws IOException {
        String path=e.getRequestURI().getPath(), method=e.getRequestMethod();
        if(!Set.of("/employees","/updates/employees","/health").contains(path)) { HttpUtil.send(e,404,"text/plain","Not found"); return; }
        if(!Set.of("GET","PUT","POST").contains(method)) { HttpUtil.send(e,405,"text/plain","Method not allowed"); return; }
        byte[] body=e.getRequestBody().readNBytes(8193);
        if(body.length>8192) { HttpUtil.send(e,413,"text/plain","Body too large"); return; }
        String correlation=UUID.randomUUID().toString();
        inFlight.put(correlation,String.valueOf(e.getRemoteAddress()));
        e.getResponseHeaders().set("X-Correlation-Id",correlation);
        boolean read=method.equals("GET");
        var lock=read?gate.readLock():gate.writeLock(); lock.lock();
        try {
            String accept=Optional.ofNullable(e.getRequestHeaders().getFirst("Accept")).orElse("application/json");
            String key=e.getRequestURI().toASCIIString()+"|"+accept;
            boolean eligible=read&&path.equals("/employees")&&!Optional.ofNullable(e.getRequestHeaders().getFirst("Cache-Control")).orElse("").contains("no-cache");
            Cached hit=cache.get(key);
            if(eligible&&hit!=null&&hit.expires()>System.nanoTime()) { send(e,200,hit,"HIT"); return; }
            int index=Math.floorMod(next.getAndIncrement(),2);
            HttpResponse<byte[]> response=null;
            // Retry reads on transport failure; writes are never replayed automatically.
            for(int attempt=0;attempt<(read?2:1);attempt++) {
                int port=8081+(index+attempt)%2;
                var builder=HttpRequest.newBuilder(URI.create("http://127.0.0.1:"+port+e.getRequestURI().toASCIIString())).timeout(Duration.ofSeconds(5)).header("Accept",accept).header("X-Correlation-Id",correlation);
                String ct=e.getRequestHeaders().getFirst("Content-Type"); if(ct!=null) builder.header("Content-Type",ct);
                builder.method(method,HttpRequest.BodyPublishers.ofByteArray(body));
                try { response=client.send(builder.build(),HttpResponse.BodyHandlers.ofByteArray()); break; }
                catch(IOException ex) { if(attempt==(read?1:0)) { HttpUtil.send(e,502,"text/plain","Warehouse unavailable; write outcome may be unknown"); return; } }
                catch(InterruptedException ex) { Thread.currentThread().interrupt(); HttpUtil.send(e,503,"text/plain","Interrupted"); return; }
            }
            if(!read) cache.clear();
            Cached result=new Cached(response.body(),response.headers().firstValue("Content-Type").orElse("application/octet-stream"),response.headers().firstValue("X-Warehouse").orElse("unknown"),System.nanoTime()+Duration.ofSeconds(5).toNanos());
            if(eligible&&response.statusCode()==200) {
                if(cache.size()>=1000) cache.clear(); cache.put(key,result);
            }
            send(e,response.statusCode(),result,eligible?"MISS":"BYPASS");
            System.out.printf("%s %s %s -> %s status=%d%n",correlation,method,e.getRequestURI(),result.node(),response.statusCode());
        } finally { lock.unlock(); inFlight.remove(correlation); }
    }
    private void send(HttpExchange e,int status,Cached c,String cacheStatus) throws IOException {
        e.getResponseHeaders().set("Content-Type",c.type()); e.getResponseHeaders().set("X-Warehouse",c.node());
        e.getResponseHeaders().set("X-Cache",cacheStatus); e.getResponseHeaders().set("Cache-Control","no-store");
        e.sendResponseHeaders(status,c.body().length); e.getResponseBody().write(c.body());
    }
}
