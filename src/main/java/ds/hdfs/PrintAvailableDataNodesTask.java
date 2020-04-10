package ds.hdfs;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class PrintAvailableDataNodesTask implements Runnable {

    protected List<String> availableDataNodes;

    public PrintAvailableDataNodesTask(NameNode nameNode){
        ConcurrentHashMap<String, Instant> heartbeats = nameNode.heartbeatTimestamps;
        Enumeration<String> dataNodeKeysEnum = heartbeats.keys();

        this.availableDataNodes = Collections.list(dataNodeKeysEnum).stream()
                .filter(d -> Duration.between(heartbeats.get(d), Instant.now()).toMillis() < 5000)
                .collect(Collectors.toCollection(ArrayList::new));
    }

    @Override
    public void run() {
        for(String dataNode : this.availableDataNodes){
            System.out.println(dataNode + " is connected");
        }
    }
}
