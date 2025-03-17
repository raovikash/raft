package distill.raft;

import org.json.JSONArray;
import org.json.JSONObject;

import java.util.*;

import static distill.raft.Actions.*;
import static distill.raft.Events.*;

enum Status {
    LEADER,
    FOLLOWER,
    CANDIDATE
}

class FollowerInfo {
    /**
     * Name of followe
     **/
    String follower_id;
    /**
     * Represents the leader's knowledge (or guess) about the follower's log
     * length.
     * <p>
     * A newly elected leader starts with not knowing the length of a follower's
     * log, so it sets logLength to the length of its own log to begin with,
     * and isLogLengthKNown to false (See below) <br>
     * <br>
     * Upon receipt of an AppendResp, the logLength is set to the msg["index"],
     * and isLogLengthKnown to msg["success"]. <br><br>
     *
     * It is only when isLogLengthKnown is true is the logLength taken seriously;
     * the leader has an accurate knowledge of the lag between the follower's log
     * and its own. Also, the length is taken into account in updateCommitted.<br>
     * When isLogLengthKnown is false, an AppendReq is sent with an empty entry
     * list, but with all other information. (@see sendAppends)
     */
    int logLength;
    boolean isLogLengthKnown;

    /**
     * Set when an AppendReq is sent and reset when an AppendResp is received.
     * This helps with batching.
     */
    boolean requestPending;

    /**
     * Set when Timeout message arrives for this follower, in onTimeout
     * Reset (to false) everytime an AppendRequest is sent out, and every time
     * an AppendResponse is received.
     */
    boolean heartbeatTimerExpired;

    FollowerInfo(String id, int logLength, boolean requestPending, boolean isLogLengthKnown, boolean heartbeatTimerExpired) {
        this.follower_id = id;
        this.logLength = logLength;
        this.requestPending = requestPending;
        this.isLogLengthKnown = isLogLengthKnown;
        this.heartbeatTimerExpired = heartbeatTimerExpired;
    }
}

public class Raft {
    static Action IGNORE_MSG = new Action() {
    };
    String myId;
    final List<String> siblings;
    final int quorumSize;
    final HashMap<String, Object> kv = new HashMap<>();
    JSONArray log = new JSONArray();
    int numCommitted = 0;
    int numApplied = 0;
    // kv:  key -> (log index,  value)
    Status status;
    Map<String, FollowerInfo> followers = new HashMap<>();
    int term;

    // votedFor is  null or the id of any of the members
    String votedFor;

    int numVotes;


    Map<String, JSONObject> pendingResponses = null;

    public Raft(String myId, List<String> members, boolean isLeader) {
        this.myId = myId;
        this.siblings = members.stream().filter(id -> ! id.equals(myId)).toList();
        quorumSize = members.size() / 2 + 1;
        term = isLeader ? 1 : 0;
        status = isLeader ? Status.LEADER : Status.FOLLOWER;
        numVotes = 0;
        votedFor = null;
    }

    Actions becomeLeader() {
        var actions = cancelAllTimers();
        this.status = Status.LEADER;
        this.followers = new HashMap<>();
        for (String fol : siblings) {
            FollowerInfo fi = new FollowerInfo(fol, log.length(),
                    false, false, false);
            followers.put(fol, fi);
            actions.add(new SetAlarm(fol));
        }
        // This should technically not be required, because becomeLeader() should follow after becomeCandidate().
        // But it is useful for tests.
        this.votedFor = myId;
        this.pendingResponses = new HashMap<>();
        return actions; // There will be actions in a later exercise.
    }

    Actions becomeFollower() {
        var actions = cancelAllTimers();
        actions.add(new SetAlarm(ELECTION));
        status = Status.FOLLOWER;
        followers = null;
        //TODO: reset votedFor and numVotes
        votedFor = null;
        numVotes = 0;
        return actions; // There will be actions in a later exercise.
    }

    Actions becomeCandidate() {
        // TODO: cancel all timers
        // TODO: increment term if not already a candidate., change status to CANDIDATE
        // TODO: SendAction voteReq message (see mkVoteReqMsg for boilerplate) to all siblings
        // TODO: remember to vote for oneself.
        // TODO: reset election timer

        Actions actions = cancelAllTimers();
        if (!isCandidate()) {
            term++;
            status = Status.CANDIDATE;
        }
        for (var fol: siblings) {
            actions.add(mkVoteReqMsg(fol, logTermBeforeIndex(log.length())));
        }
        votedFor = myId;
        numVotes = 1;
        actions.add(new SetAlarm(ELECTION));
        return actions;
    }

    Actions cancelAllTimers() {
        Actions actions = new Actions();
        for(var fol: siblings) {
            actions.add(new CancelAlarm(fol));
        }
        actions.add(new CancelAlarm(ELECTION));
        return actions;
    }

    Action mkVoteReqMsg(String to, int lastLogTerm) {
        assert !isLeader();

        var msg = mkMsg(
                // TODO: make message with fields
                //      from, to, type, term, log_length, last_log_term
                "from", myId,
                "to", to,
                "type", VOTE_REQ,
                "term", term,
                "log_length", log.length(),
                "last_log_term", lastLogTerm
        );
        return new Send(msg);
    }


    public Action mkReply(JSONObject msg, Object... extraKeyValues) {
        JSONObject reply = mkMsg(
                "from", myId,
                "to", msg.get("from"),
                "term", term);
        if (msg.has("reqid")) {
            reply.put("reqid", msg.get("reqid"));
        }
        String reqType = msg.getString("type");
        String responseType = Events.responseType.get(reqType);
        if (responseType == null) {
            throw new RuntimeException("msg type error: " + msg);
        }
        reply.put("type", responseType);

        for (int i = 0; i < extraKeyValues.length; i += 2) {
            var value = extraKeyValues[i + 1];
            reply.put((String) extraKeyValues[i], value);
        }

        return new Send(reply);
    }

    public boolean isLeader() {
        return status == Status.LEADER;
    }

    public boolean isCandidate() {
        return status == Status.CANDIDATE;
    }

    int logTermBeforeIndex(int index) {
        if (!log.isEmpty() && index > 0) {
            var entry = log.getJSONObject(index-1);
            return entry.getInt("term");
        }
        return 0;
    }

    Actions onAppendReq(JSONObject msg) {
        assert !isLeader();
        int msgIndex = (int) msg.get("index");
        var msgEntries = (JSONArray) msg.get("entries");
        var prevLogTerm = (int) msg.get("prev_log_term");
        Action toSend;
        if (msgIndex > log.length()) {
            // Ask leader to back up
            toSend = mkReply(msg, "success", false, "index", log.length());
        } else {
            int myPrevLogTerm = logTermBeforeIndex(msgIndex);
            // If the prev entry's term did not match, tell the leader to back up one item
            if (prevLogTerm != myPrevLogTerm) {
                toSend = mkReply(msg, "success", false, "index", msgIndex - 1);
            } else {
                if (msgIndex == log.length()) {
                    log.putAll(msgEntries);
                    toSend = mkReply(msg, "success", true, "index", log.length());
                } else { // msgIndex < log.length()
                    // chop tail until msgIndex, then add msgEntries
                    var newlog = new JSONArray(log.length());
                    for (int i = 0; i < msgIndex; i++) {
                        newlog.put(log.get(i));
                    }
                    log = newlog;
                    log.putAll(msgEntries);
                    toSend = mkReply(msg, "success", true, "index", log.length());
                }
            }
        }
        var actions = new Actions(toSend);

        if (!isLeader()) {
            var numCommittedAtLeader = msg.getInt("num_committed");
            if (numCommittedAtLeader > numCommitted) {
                numCommitted = numCommittedAtLeader;
                actions.add(onCommit());
            }
            actions.add(new SetAlarm(ELECTION));
        }
        return actions;
    }

    Actions onAppendResp(JSONObject msg) {
        assert isLeader();
        int msgIndex = msg.getInt("index");
        assert msgIndex <= log.length() : msgIndex;
        var fi = followers.get(msg.getString("from"));
        fi.logLength = msgIndex;
        fi.requestPending = false;
        fi.isLogLengthKnown = msg.getBoolean("success");
        var actions = new Actions(new SetAlarm(fi.follower_id)); // heartbeat timer reset
        if (updateNumCommitted()) {
            actions.add(onCommit());
        }
        return actions;
    }
    
    boolean updateNumCommitted() {
        // This method is called every time an append response comes, and
        // we check to see how much of the log has been committed at all.
        // if the number committed has changed, it returns true.

        // This is how to
        // Suppose the leader and followers' log lengths are as follows :
        // [10, 5, 4, 8, 10].
        //
        // The last follower has caught up, but tht others are lagging. To find out the number
        // of entries present in a majority of servers (that can be considered
        // committed), we sort the list (in descending order), pick a quorum-sized
        // slice from the top lengths, and use the last length (and smallest) element
        // in this list.
        // In this example, the sorted list is [10, 10, 8, 5, 4].
        // The top quorum-sized slice is [10, 10, 8]
        // 8 is the last and the smallest of this slice.
        // Regardless of which triple combination is chosen, we are
        // guaranteed that at least one server has 8 entries in its log.

        assert isLeader();

        List<Integer> lengths = new ArrayList<>();
        lengths.add(log.length());
        for (FollowerInfo fi : followers.values()) {
            if (fi.isLogLengthKnown) {
                lengths.add(fi.logLength);
            }
        }
        if (lengths.size() < quorumSize) {
            return false;
        }
        lengths.sort(Collections.reverseOrder());

        int newNumCommitted = lengths.get(quorumSize - 1);
        assert newNumCommitted >= numCommitted;

        var lastTerm = logTermBeforeIndex(newNumCommitted);
        boolean retval = false;
        if (lastTerm == term) {
            // The leader can commit older terms only if one of its own entries
            // is present on on a majority Otherwise, if the leader gets deposed,
            // some other leader may impose its log, with different entries. We
            // can consider an entry stable only when we are sure that a new
            // leader will have that entry present (and by induction, all earlier
            // entries)
            retval = newNumCommitted > numCommitted;
            numCommitted = newNumCommitted;
        }
        return retval;
    }


    Action onVoteReq(JSONObject msg) {
        var voteGranted = false;
        // TODO: Reply "vote_granted" == true
        //    if votedFor is null and
        //         either msg["last_log_term"] > this.lastLogTerm
        //        or     msg["last_log_term"] == this.lastLogTerm && msg["log_length"] >= this.logLength
        // TODO: In all other cases, "vote_granted" == false
        // TODO: reply using mkReply(msg)
        // TODO: If vote is granted, set votedFor.
        if (votedFor == null) {
            int lastLogTerm = msg.getInt("last_log_term");
            int logLength = msg.getInt("log_length");
            if (lastLogTerm > logTermBeforeIndex(log.length()) ||
                    (lastLogTerm == logTermBeforeIndex(log.length()) && logLength >= log.length())) {
                voteGranted = true;
                votedFor = msg.getString("from");
            }
        }
        return mkReply(msg, "vote_granted", voteGranted);
    }

    Actions onVoteResp(JSONObject ignoredMsg) {
        Actions actions = NO_ACTIONS;
        if (isCandidate()) {
            //     if ++numVotes meets quorum sizes, become leader
            //     return appropriate actions from becoming leader
            ++numVotes;
            if (numVotes >= quorumSize) {
                actions = becomeLeader();
            }
        }
        return actions;
    }

    Action apply(int index, JSONObject entry) {
        var key = entry.getString("key");
        var cmd = entry.getString("cmd");
        Action action = NO_ACTIONS;

        JSONObject clientMsg = null;

        if (isLeader()) {
            String reqid = entry.getString("cl_reqid");
            clientMsg = pendingResponses.get(reqid);
            pendingResponses.remove(reqid);
        }

        if (cmd.equals("W")) {
            var value = entry.get("value");
            kv.put(key, value);
            if (clientMsg != null) {
                action = mkReply(clientMsg, "client_msg", clientMsg,
                        "index", index);
            }
        }
        return action;
    }
    Actions onCommit() {
        var actions = new Actions();
        assert numCommitted >= numApplied;
        for (int index = numApplied; index < numCommitted; index++) {
            Action action = apply(index, (JSONObject) log.get(index));
            if (action != NO_ACTIONS) {
                actions.add(action);
            }
        }
        numApplied = numCommitted;
        return actions;
    }

    static JSONObject mkMsg(Object... kvpairs) {
        JSONObject jo = new JSONObject();
        if (kvpairs.length % 2 != 0) {
            throw new RuntimeException("kvpairs must be even numbered");
        }
        for (int i = 0; i < kvpairs.length; i += 2) {
            var value = kvpairs[i + 1];
            jo.put((String) kvpairs[i], value);
        }
        return jo;
    }

    Action mkAppendMsg(String to, int index, boolean emptyEntries) {
        assert isLeader();

        var msg = mkMsg(
                "from", myId,
                "to", to,
                "type", APPEND_REQ,
                "index", index,
                "num_committed", numCommitted,
                "term", term
        );
        var prevLogTerm = logTermBeforeIndex(index);
        msg.put("prev_log_term", prevLogTerm);
        // create a slice.
        if (emptyEntries) {
            msg.put("entries", new JSONArray());
        } else {
            var entries = new JSONArray(log.length() - index);
            for (int i = index; i < log.length(); i++) {
                entries.put(log.get(i));
            }
            msg.put("entries", entries);
        }
        return new Send(msg);
    }

    Actions sendAppends() {
        if (!isLeader()) {
            return NO_ACTIONS;
        }
        var actions = new Actions();

        for (var fi : followers.values()) {
            // If the heartbeat timer has expired, we send a message anyway, and reload the timer.
            // If a request is not pending and the follower is lagging, send a message, reload the timer
            // If a message is going to be sent, send an empty entries packet if not fi.isLogLengthKnown.
            if (fi.heartbeatTimerExpired ||
                    (!fi.requestPending && fi.logLength < log.length())) {
                fi.heartbeatTimerExpired = false;
                actions.add(new SetAlarm(fi.follower_id));
                // Send empty packet if follower has not yet matched. We are still backtracking
                actions.add(mkAppendMsg(fi.follower_id, fi.logLength, !fi.isLogLengthKnown));
                fi.requestPending = true;
            }
        }
        return actions;
    }

    /** Election timeout messages look like this
     * <pre>
     *    {type: 'TIMEOUT', 'name': 'ELECTION'}
     * </pre>
     * Heartbeat timeout messages look like this:
     * <pre>
     *    {type: 'TIMEOUT', 'name': 'S2'} , where S2 is the name of a follower.
     * </pre>
     **/
    Actions onTimeout(JSONObject msg) {
        // is it an election timer?
        var timer = msg.getString("name");
        if (isLeader()) {
            if (timer.equals(ELECTION)) {
                return NO_ACTIONS;
            }
            var fi = followers.get(timer);
            fi.heartbeatTimerExpired = true;
        } else {
            if (timer.equals(ELECTION)) {
                return becomeCandidate();
            }
        }
        return NO_ACTIONS;
    }

    Action checkTerm(JSONObject msg) {
        var actions = NO_ACTIONS;
        var msgTerm = msg.getInt("term");
        if (msgTerm > term) {
            term = msgTerm;
            if (isLeader() || isCandidate()) {
                actions = becomeFollower();
            }
        } else if (msgTerm < term) {
            return IGNORE_MSG;
        }
        return actions;
    }

    public Actions start() {
        if (isLeader()) {
            return becomeLeader();
        }
        return becomeFollower();
    }

    Action onClientCommand(JSONObject msg) {
        Action action = NO_ACTIONS;
        if (!isLeader()) {
            action = mkReply(msg, "errmsg", "Not a leader");
        } else {
            switch (msg.getString("cmd")) {
                case "R" -> {
                    var key = msg.getString("key");
                    var value = kv.get(key);
                    action = mkReply(msg, "value", value);
                }
                case "W" -> {
                    pendingResponses.put(msg.getString("reqid"), msg);
                    replicate(msg);
                }
                default -> throw new RuntimeException("Unknown cmd " + msg);
            }
        }
        return action;
    }

    void replicate(JSONObject msg) {
        var entry = mkMsg(
    "term", term,
            "cl_reqid", msg.getString("reqid"),
            "key", msg.get("key"),
            "cmd", msg.get("cmd"),
            "value", msg.get("value")
        );
        log.put(entry);
    }

    Actions processMsg(JSONObject msg) {
        var msgType = msg.getString("type");
        var actions = new Actions();
        if (!(msgType.equals(CMD_REQ) || msgType.equals(TIMEOUT))) {
            var action = checkTerm(msg);
            if (action == IGNORE_MSG) {
                return NO_ACTIONS;
            }
        }

        switch (msgType) {
            case APPEND_REQ -> actions.add(onAppendReq(msg));
            case APPEND_RESP -> actions.add(onAppendResp(msg));
            case VOTE_REQ -> actions.add(onVoteReq(msg));
            case VOTE_RESP -> actions.add(onVoteResp(msg));
            case CMD_REQ -> actions.add(onClientCommand(msg));
            case TIMEOUT -> actions.add(onTimeout(msg));
            default -> throw new RuntimeException("Unknown msg type " + msgType);
        }
        if (isLeader()) {
            actions.add(sendAppends());
        }
        return actions;
    }

}