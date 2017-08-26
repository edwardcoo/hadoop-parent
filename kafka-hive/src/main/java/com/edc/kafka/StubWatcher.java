package com.edc.kafka;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

/**
 * Created by Mtime on 2017/8/24.
 */
public class StubWatcher implements Watcher {
    @Override
    public void process(WatchedEvent event) {

    }
}
