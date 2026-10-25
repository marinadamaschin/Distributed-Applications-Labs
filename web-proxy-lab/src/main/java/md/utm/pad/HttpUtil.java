package md.utm.pad;
import com.sun.net.httpserver.HttpExchange;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.net.URLDecoder;
import java.util.*;

final class HttpUtil {
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
