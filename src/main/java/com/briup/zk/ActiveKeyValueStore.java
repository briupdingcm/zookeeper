package com.briup.zk;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class ActiveKeyValueStore implements Watcher{
	private static final int SESSION_TIMEOUT = 5000;
	private static final Charset CHARSET = Charset.forName("UTF-8");
	private ZooKeeper zk = null;
	private CountDownLatch connectedSignal = new CountDownLatch(1);
	
	public void connect(String hosts)throws IOException, InterruptedException{
		zk = new ZooKeeper(hosts, SESSION_TIMEOUT, this);
		connectedSignal.await();
	}

	@Override
	public void process(WatchedEvent event) {
		if(event.getState() == KeeperState.SyncConnected){
			connectedSignal.countDown();
		}
		
	}

	public void write(String path, String value)throws InterruptedException, KeeperException{
		Stat stat = zk.exists(path, false);
		if(stat == null){
			zk.create(path, value.getBytes(CHARSET), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			
		}else{
			zk.setData(path, value.getBytes(CHARSET), -1);
		}
	}
	
	public String read(String path, Watcher watcher)throws InterruptedException, KeeperException{
		byte[] data = zk.getData(path, watcher, null);
		return new String(data, CHARSET);
	}
}
