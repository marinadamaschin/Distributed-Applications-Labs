package md.utm.pad;
import com.sun.net.httpserver.HttpExchange;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.net.URLDecoder;
import java.util.*;

final class HttpUtil {
    static String correlation(HttpExchange e) {
        String id = e.getRequestHeaders().getFirst("X-Correlation-Id");
        if (id == null || id.isBlank()) return UUID.randomUUID().toString();
        if (id.length() > 128 || !id.matches("[A-Za-z0-9._:/-]+"))
            throw new IllegalArgumentException("Invalid X-Correlation-Id (max 128 ASCII letters/digits or ._:/-)");
        return id;
    }
    static String negotiate(String accept) {
        if (accept == null || accept.isBlank()) return "application/json";
        double json = quality(accept, "application/json"), xml = quality(accept, "application/xml");
        if (json <= 0 && xml <= 0) return null;
        return xml > json ? "application/xml" : "application/json";
    }
    private static double quality(String accept, String type) {
        int specificity = -1; double quality = 0;
        for (String range : accept.toLowerCase(Locale.ROOT).split(",")) {
            String[] parts = range.trim().split(";"); String media = parts[0].trim();
            int rank = media.equals(type) ? 2 : media.equals("application/*") ? 1 : media.equals("*/*") ? 0 : -1;
            if (rank < 0) continue;
            double q = 1;
            for (int i = 1; i < parts.length; i++) if (parts[i].trim().startsWith("q=")) {
                try { q = Double.parseDouble(parts[i].trim().substring(2)); }
                catch (NumberFormatException ex) { q = 0; }
            }
            if (!Double.isFinite(q) || q < 0 || q > 1) q = 0;
            if (rank > specificity) { specificity = rank; quality = q; }
            else if (rank == specificity) quality = Math.max(quality, q);
        }
        return quality;
    }
    static void send(HttpExchange e,int status,String type,String body) throws IOException {
        byte[] bytes=body.getBytes(StandardCharsets.UTF_8);
        e.getResponseHeaders().set("Content-Type",type+"; charset=utf-8");
        e.sendResponseHeaders(status,bytes.length); e.getResponseBody().write(bytes);
    }
    static Map<String,String> query(HttpExchange e) {
        Map<String,String> result=new HashMap<>(); String raw=e.getRequestURI().getRawQuery();
        if(raw!=null) for(String part:raw.split("&")) {
            String[] kv=part.split("=",2);
            result.put(URLDecoder.decode(kv[0],StandardCharsets.UTF_8),kv.length==2?URLDecoder.decode(kv[1],StandardCharsets.UTF_8):"");
        } return result;
    }
    static String xml(String s) { return s.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;").replace("\"","&quot;"); }
    static String json(String s) { return "\""+s.replace("\\","\\\\").replace("\"","\\\"").replace("\n","\\n").replace("\r","\\r").replace("\t","\\t")+"\""; }
    static String employee(EmployeeStore.Employee e,boolean xml) {
        return xml?"<employee><id>"+e.id()+"</id><name>"+xml(e.name())+"</name></employee>":"{\"id\":"+e.id()+",\"name\":"+json(e.name())+"}";
    }
}
