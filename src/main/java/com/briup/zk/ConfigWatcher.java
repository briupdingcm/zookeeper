package com.briup.zk;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;

public class ConfigWatcher implements Watcher {
	private ActiveKeyValueStore store;
	private static final String PATH = "/config";

	public ConfigWatcher(String hosts)throws IOException, InterruptedException{
		store = new ActiveKeyValueStore();
		store.connect(hosts);
	}
	
	public void displayConfig()throws InterruptedException, KeeperException{
		String value = store.read(PATH, this);
		System.out.printf("Read %s as %s\n", PATH, value);
	}

	@Override
	public void process(WatchedEvent event) {
		if(event.getType() == EventType.NodeDataChanged){
			try{
				displayConfig();
			}catch(InterruptedException e){
				System.err.println("Interrupted. Exiting.");
				Thread.currentThread().interrupt();
			} catch (KeeperException e) {
				System.err.printf("KeeperException: %s. Exiting.\n", e);
			}
		}
	}

	public static void main(String[] args) throws Exception{
		ConfigWatcher watcher = new ConfigWatcher("127.0.0.1");
		watcher.displayConfig();
		Thread.sleep(Long.MAX_VALUE);
	}

}
