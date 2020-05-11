package com.briup.zk;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsReader {

	public static void main(String[] args) throws IOException {
		Map<String, Integer> res = new TreeMap<String, Integer>();
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://server1.cloud.briup.com:9000");
		Path path = new Path("/data/weather/999999-99999-2009");
		FileSystem fs = path.getFileSystem(conf);
		FileStatus fss = fs.getFileStatus(path);
		System.out.println("block size: " + fss.getBlockSize()/(1024*1024));
		System.out.println("replication: " + fss.getReplication());
		for(BlockLocation loc :fs.getFileBlockLocations(fss, 0, fss.getLen())){
			System.out.print(loc.getOffset()/(1024*1024) + " : ");
			for(String host : loc.getNames()){
				System.out.print(host + ", ");
				if(res.get(host) != null){
					res.put(host, res.get(host) + 1);
				}else{
					res.put(host, 0);
				}
			}
			System.out.println();
		}
		for(String key : res.keySet())
		System.out.println(key + ": " + res.get(key));

		fs.close();
	}

}
