package distill.echo3;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.json.JSONObject;

public class Client {
    public static void main(String[] args) throws Exception{
        String myid = null;
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("--id")) {
                myid = args[i + 1];
                break;
            }
        }
        if (myid == null) {
            throw new RuntimeException("--id not specified");
        }

        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        write("5", "8", reader);
        write("10", "20", reader);
        read("5", reader);
    }

    static void write(String key, Object value, BufferedReader reader) throws IOException {
        var msg = new JSONObject();
        msg.put("to", "server_3");
        msg.put("from", "client_3");
        msg.put("type", "CMD_REQ");
        msg.put("cmd", "W");
        msg.put("key", key);
        msg.put("value", value);
        System.out.println(msg);
        // System.out.flush();
        // // Get response
        // var line = reader.readLine();
        // var rcvdMsg = new JSONObject(line);
        // System.out.println(rcvdMsg);
        System.out.flush();
    }

    static void read(String key, BufferedReader reader) throws IOException {
        var msg = new JSONObject();
        msg.put("to", "server_3");
        msg.put("from", "client_3");
        msg.put("type", "CMD_REQ");
        msg.put("cmd", "R");
        msg.put("key", key);
        System.out.println(msg);
        // System.out.flush();
        // // Get response
        // var line = reader.readLine();
        // var rcvdMsg = new JSONObject(line);
        System.out.flush();
        // System.out.println("rcvdMsg = " + rcvdMsg);
    }
}
