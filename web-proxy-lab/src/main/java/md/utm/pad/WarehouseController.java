package md.utm.pad;
import com.sun.net.httpserver.*;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public final class WarehouseController implements HttpHandler {
    private final EmployeeStore store; private final String node;
    public WarehouseController(EmployeeStore store,String node) { this.store=store; this.node=node; }
    public void handle(HttpExchange e) throws IOException {
        String correlation = HttpUtil.correlation(e);
        e.getResponseHeaders().set("X-Correlation-Id", correlation);
        e.getResponseHeaders().set("X-Warehouse",node);
        try {
            dispatch(e);
        } catch (IOException ex) {
            HttpUtil.send(e,503,"text/plain","Storage unavailable; write outcome may be unknown");
        } finally {
            System.out.printf("DW=%s correlation=%s method=%s uri=%s status=%d%n",
                node, correlation, e.getRequestMethod(), e.getRequestURI(), e.getResponseCode());
        }
    }
    private void dispatch(HttpExchange e) throws IOException {
        String method=e.getRequestMethod(), path=e.getRequestURI().getPath();
        if(path.equals("/health")) {
            if (!method.equals("GET")) { HttpUtil.send(e,405,"text/plain","GET required"); return; }
            store.health(); HttpUtil.send(e,200,"text/plain",node+" OK"); return;
        }
        Map<String,String> q=HttpUtil.query(e);
        String type=HttpUtil.negotiate(e.getRequestHeaders().getFirst("Accept"));
        if (type == null) { HttpUtil.send(e,406,"text/plain","Supported formats: application/json, application/xml"); return; }
        boolean xml=type.equals("application/xml");
        if(method.equals("GET") && path.equals("/updates/employees")) {
            long after = Long.parseLong(q.getOrDefault("after","0"));
            if (after < 0) throw new IllegalArgumentException("after >= 0");
            var changes=store.changes(after);
            String body=changes.stream().map(c -> xml?"<change><sequence>"+c.sequence()+"</sequence>"+HttpUtil.employee(c.employee(),true)+"</change>":"{\"sequence\":"+c.sequence()+",\"employee\":"+HttpUtil.employee(c.employee(),false)+"}").collect(Collectors.joining(xml?"":","));
            HttpUtil.send(e,200,type,xml?"<changes>"+body+"</changes>":"["+body+"]"); return;
        }
        if(!path.equals("/employees")) { HttpUtil.send(e,404,"text/plain","Not found"); return; }
        if(method.equals("GET")) {
            if(q.containsKey("id")) {
                var employee=store.get(Integer.parseInt(q.get("id")));
                if(employee==null) { HttpUtil.send(e,404,"text/plain","Employee not found"); return; }
                HttpUtil.send(e,200,type,HttpUtil.employee(employee,xml)); return;
            }
            int offset=Integer.parseInt(q.getOrDefault("offset","0")),limit=Integer.parseInt(q.getOrDefault("limit","100"));
            if(offset<0||limit<1||limit>1000) throw new IllegalArgumentException("offset >= 0, limit 1..1000");
            String body=store.list(offset,limit).stream().map(v -> HttpUtil.employee(v,xml)).collect(Collectors.joining(xml?"":","));
            HttpUtil.send(e,200,type,xml?"<employees>"+body+"</employees>":"["+body+"]");
        } else if(method.equals("PUT")||method.equals("POST")) {
            // Explicit form encoded write contract; response format remains independently selectable.
            String ct=e.getRequestHeaders().getFirst("Content-Type");
            if(ct==null||!ct.startsWith("application/x-www-form-urlencoded")) { HttpUtil.send(e,415,"text/plain","Use application/x-www-form-urlencoded: id=1&name=Ana"); return; }
            byte[] bytes=e.getRequestBody().readNBytes(8193);
            if(bytes.length>8192) { HttpUtil.send(e,413,"text/plain","Body too large"); return; }
            Map<String,String> fields=new HashMap<>();
            for(String part:new String(bytes,java.nio.charset.StandardCharsets.UTF_8).split("&")) {
                String[] kv=part.split("=",2); if(kv.length!=2) throw new IllegalArgumentException("Expected id and name");
                fields.put(kv[0],java.net.URLDecoder.decode(kv[1],java.nio.charset.StandardCharsets.UTF_8));
            }
            int id=Integer.parseInt(fields.getOrDefault("id","0")); String name=fields.getOrDefault("name","").trim();
            if(id<=0||name.isEmpty()||name.length()>100||name.chars().anyMatch(c->c<32)) throw new IllegalArgumentException("Positive id and name of 1..100 printable characters required");
            var employee=new EmployeeStore.Employee(id,name);
            if(method.equals("PUT")) { boolean created = store.put(employee);
                if (created) e.getResponseHeaders().set("Location", "/employees?id="+id);
                HttpUtil.send(e,created?201:200,type,HttpUtil.employee(employee,xml)); }
            else { long sequence=store.propose(employee); HttpUtil.send(e,202,"text/plain","Proposed update sequence="+sequence); }
        } else { e.getResponseHeaders().set("Allow","GET, PUT, POST"); HttpUtil.send(e,405,"text/plain","Method not allowed"); }
    }
}
