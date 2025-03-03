package distill.raft;
import java.util.*;
import static distill.Utils.*;
import static distill.raft.Actions.*;

public class Server {
    final Raft raft;

    /**
     * THe key can be the id of a follower or the string "ELECTION"
     * The value is a Timer object returned by Utils.setTimeout
     */
    final Map<String, Timer> timers;
    static final int ELECTION_DURATION_SEC = 30;
    static final int HEARTBEAT_DURATION_SEC = 10;



    public Server(String id, List<String> members, boolean isLeader) {
        raft = new Raft(id, members, isLeader);
        timers = new HashMap<>();
    }

    void perform(Action action) {
        if (action instanceof Actions actions) {
            for (var act : actions.todos) {
                perform(act); // recursive
            }
        } else {
            if (action instanceof Send sendAction) {
                send(sendAction.msg());
            } else if (action instanceof SetAlarm setAlarm) {
                var name = setAlarm.name();
                if (timers.containsKey(name)) {
                    var timer = timers.get(name);
                    timer.cancel();
                }
                double durationSec;
                if (name.equals(ELECTION)) {
                    // TODO: durationSec is some random number between
                    //    ELECTION_DURATION_SEC and 2 * ELECTION_DURATION_SEC
                    durationSec = ???
                } else {
                    durationSec = HEARTBEAT_DURATION_SEC;
                }
                timers.put(name, setTimeout(durationSec, name));
            } else if (action instanceof CancelAlarm cancelAlarm) {
                var name = cancelAlarm.name();
                if (timers.containsKey(name)) {
                    var timer = timers.get(name);
                    timer.cancel();
                    timers.remove(name);
                }
            }
        }
    }
    void loop() {
        var actions = raft.start();
        perform(actions);

        info("Raft server " + raft.myId + " started as " +
                (raft.isLeader() ? "leader" : "follower"));

        //noinspection InfiniteLoopStatement
        while (true) {
            var msg = recv();
            actions = raft.processMsg(msg);
            perform(actions);
        }
    }

    public static void main(String[] args) {
        String id = nodeId();
        boolean isLeader = id.endsWith("1");
        var server = new Server(id, siblingNodes(), isLeader);
        server.loop();
    }
}
