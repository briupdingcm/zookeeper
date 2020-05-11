package com.briup.zk;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;

public class DeleteGroup implements Watcher {
	private static final int  SESSION_TIMEOUT = 5000;
	private ZooKeeper zk = null;
	private CountDownLatch connectedSignal = new CountDownLatch(1);
	
	public void connect(String hosts)throws IOException, InterruptedException{
		zk = new ZooKeeper(hosts, SESSION_TIMEOUT, this);
		connectedSignal.await();
	}
	public DeleteGroup() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public void process(WatchedEvent event) {
		if(event.getState() == KeeperState.SyncConnected){
			connectedSignal.countDown();
		}
	}

	public void delete(String groupName)throws KeeperException, InterruptedException{
		String path = "/" + groupName;
		try{
			List<String> children = zk.getChildren(path, false);
			for(String child:children){
				zk.delete(path + "/" + child, -1);
			}
			zk.delete(path, -1);
		}catch(KeeperException.NoNodeException e){
			System.out.printf("Group %s does not exist\n", groupName);
			System.exit(1);
		}
	}
	
	public void close() throws Exception{
		zk.close();
	}
	
	public static void main(String[] args)throws Exception {
		DeleteGroup delete = new DeleteGroup();
		delete.connect("127.0.0.1");
		delete.delete("hbase");
		delete.close();
	}

}
