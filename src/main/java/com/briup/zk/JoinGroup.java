package com.briup.zk;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

public class JoinGroup implements Watcher {
	private static final int SESSION_TIMEOUT = 5000;
	private ZooKeeper zk;
	private CountDownLatch connectedSignal = new CountDownLatch(1);
	
	public void join(String groupName, String memberName)throws KeeperException, InterruptedException{
		String path = "/" + groupName + "/" + memberName;
		String createdPath = zk.create(path, null, Ids.OPEN_ACL_UNSAFE, 
				CreateMode.EPHEMERAL);
		System.out.println("Created " + createdPath);
	}
	
	public void connect(String hosts)throws IOException, InterruptedException{
		zk = new ZooKeeper(hosts, SESSION_TIMEOUT, this);
		connectedSignal.await();
	}
	public JoinGroup() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public void process(WatchedEvent event) {
		if(event.getState() == KeeperState.SyncConnected){
			connectedSignal.countDown();
		}
	}

	public void close()throws InterruptedException{
		zk.close();
	}
	public static void main(String[] args) throws Exception{
		JoinGroup group = new JoinGroup();
		group.connect("127.0.0.1");;
		group.join("hbase", "kevin");
		Thread.sleep(Long.MAX_VALUE);
	}

}
