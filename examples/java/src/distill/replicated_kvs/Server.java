package distill.replicated_kvs;

import static distill.Utils.*;

import org.json.JSONObject;

import java.util.*;


public class Server {
    final HashMap<String, Object> kv = new HashMap<>();
    final List<String> siblings;
    boolean isLeader;


    static final String APPEND_REQ = "APPEND_REQ";
    static final String APPEND_RESP = "APPEND_RESP";

    static final String CMD_REQ = "CMD_REQ";
    static final String CMD_RESP = "CMD_RESP";


    // reqid -> number of pending replication acks
    HashMap<String, Integer> pendingAcks = new HashMap<>();

    String nodeId;

    public Server(String nodeId, List<String> siblings, boolean isLeader) {
        this.nodeId = nodeId;
        this.isLeader = isLeader;
        this.siblings = siblings;
        if (isLeader) {
            this.pendingAcks = new HashMap<>();
        }
    }

    public void apply(JSONObject msg) {
        kv.put(msg.getString("key"), msg.get("value"));
        var reply = mkReply(msg, "from", nodeId, "to", "client");
        System.out.println(reply);
    }

    int quorumSize() {
        // System.err.println("siblings.size() = " + siblings.size());
        return (siblings.size() / 2) + 1;
    }
    /**
     * Message
     * @param msg from leader to follower
     */
    void onAppendReq(JSONObject msg) {
        if (!isLeader) {
            // System.err.println("msg in onAppendReq = " + msg);
            var key = msg.getString("key");
            var value = msg.get("value");
            kv.put(key, value);
            var reply = mkReply(msg,"type", APPEND_RESP, "key", key, "value", value);
            // System.err.println("reply in onAppendReq = " + reply);
            System.out.println(reply);
        }
    }

    void onClientCommand(JSONObject msg) {
        if (isLeader) {
            var key = msg.getString("key");
            var cmd = msg.getString("cmd");
            if (cmd.equalsIgnoreCase("R")) {
                var reply = mkReply(msg, "value", kv.get(key));
                // System.err.print("value for key " + key + " is " + kv.get(key));
                System.out.println(reply);
            } else if (cmd.equalsIgnoreCase("W")) {
                replicate(msg);
            }
        }
    }

    void replicate(JSONObject msg) {
        var reqid = msg.has("reqid") ? msg.getString("reqid") : UUID.randomUUID().toString();
        for (String sibling: siblings) {
            var appendReq = mkMsg("from", nodeId, "to", sibling, "type", APPEND_REQ, "reqid", reqid,
                "key", msg.getString("key"), "value", msg.get("value"));
            System.out.println(appendReq);
        }
    }


    void onAppendResp(JSONObject msg) {
        if (isLeader) {
            // System.err.println("msg in onAppendResp = " + msg);
            var reqid = msg.getString("reqid");
            var acks = pendingAcks.getOrDefault(reqid, 0);
            acks++;
            // System.err.println("qualsize = " + quorumSize() + " acks = " + acks);
            if (acks >= quorumSize()) {
                pendingAcks.remove(reqid);
                // System.err.println("msg = " + msg);
                apply(msg);
            } else {
                pendingAcks.put(reqid, acks);
            }
        }
    }


    // Convenience method for creating a reply to a message
    // The "from", "to" fields are flipped,  the type is flipped to the appropriate
    // response type, and the reqid is copied.
    public static JSONObject mkReply(JSONObject msg, Object... extraKeyValues) {
        JSONObject reply = new JSONObject();
        reply.put("from", nodeId());

        reply.put("to", msg.get("from"));
        if (msg.has("reqid"))
            reply.put("reqid", msg.get("reqid"));
        switch (msg.getString("type")) {
            case CMD_REQ: reply.put("type", CMD_RESP);break;
            case APPEND_REQ: reply.put("type", APPEND_RESP); break;
            default: throw new RuntimeException("msg with unknown type" + msg);
        }
        for (int i = 0; i < extraKeyValues.length; i+=2) {
            reply.put((String)extraKeyValues[i], extraKeyValues[i+1]);
        }
        return reply;
    }

    public void listen() {
        while (true) {
            JSONObject msg = recv();
            String msgType = msg.getString("type");
            switch (msgType) {
                // sends append request to followers
                // append request is only handled by follower
                case APPEND_REQ: onAppendReq(msg); break; // APPEND_RESP
                // handles append response from followers
                // append response is only handled by leader
                case APPEND_RESP: {
                    onAppendResp(msg);
                    break;
                }
                // handles client command
                // client command is only handled by leader
                // on read, leader reads from map and returns
                // on write, leads sends append request to followers
                case CMD_REQ: {
                    onClientCommand(msg);
                    break;
                }
                default: throw new RuntimeException("Unknown message type " + msgType);
            }
        }
    }


    public static void main(String[] args) {
        // Hardcode server 1 as the leader
        var isLeader = nodeId().endsWith("1");
        var server = new Server(nodeId(), siblingNodes(), isLeader);
        server.listen();
    }
}