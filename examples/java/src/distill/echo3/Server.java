package distill.echo3;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.json.JSONObject;

public class Server {
    public static final Map<String, Object> KAY_VALUE_STORE = new HashMap<>();
    public static void main(String[] args) throws Exception {
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        
        while (true) {
            var line = reader.readLine();
            
            var msg = new JSONObject(line);
            var from = msg.getString("from");
            var to = msg.getString("to");
            var type = msg.getString("type");
            var cmd = msg.getString("cmd");

            var key = msg.getString("key");
            
            var output = new JSONObject(line);
            output.put("from", "server");
            output.put("to", "client");
            output.put("type", "CMD_RESPONSE");

            if ("W".equals(cmd)) {
                var value = msg.getString("value");
                KAY_VALUE_STORE.put(key, value);
                // System.out.println(String.format("INFO: Key:%s Value:%s stored successfully", key, value));
                output.put("success", "true");
            } else if ("R".equals(cmd)) {
                Object val = KAY_VALUE_STORE.get(key);
                // System.out.println(String.format("INFO: Key:%s Value:%s", key, val));
                output.put("value", val);
                output.put("success", "true");
            } else {
                // System.out.println("INFO: Invalid command");
                output.put("success", "false");
            }
            System.out.println(output);
            System.out.flush();
        }
    }
}
