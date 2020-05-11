package com.briup.zk;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;

public class ListGroup implements Watcher {
	private final static int SESSION_TIMEOUT = 5000;
	private ZooKeeper zk;
	private CountDownLatch connectedSignal = new CountDownLatch(1);
	
	public void connect(String hosts)throws InterruptedException, IOException{
		zk = new ZooKeeper(hosts, SESSION_TIMEOUT, this);
		connectedSignal.await();
	}
	
	@Override
	public void process(WatchedEvent event) {
		if(event.getState() == KeeperState.SyncConnected){
			connectedSignal.countDown();
		}
	}

	public void list(String groupName)throws KeeperException, InterruptedException{
		String path = "/" + groupName;
		try{
			List<String> children = zk.getChildren(path, false);
			if(children.isEmpty()){
				System.out.printf("No members in group %s\n", groupName);
				System.exit(1);
			}
			for(String child : children){
				System.out.println(child);
			}
		}catch(KeeperException.NoNodeException e){
			System.out.printf("Group %s does not exist\n", groupName);
			System.exit(1);
		}
	}
	
	public void close() throws InterruptedException{
		zk.close();
	}
	public static void main(String... args)throws Exception{
		ListGroup list = new ListGroup();
		list.connect("127.0.0.1");
		list.list("");
		list.close();
	}
}
