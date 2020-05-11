package com.briup.zk;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Inet4Address;
import java.net.InetAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsWriter {

	public static void main(String[] args) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://computer1.cloud.briup.com:9000");
		FileSystem fs = FileSystem.get(conf);
		FSDataOutputStream fos = fs.append(new Path("/user/kevin/test.txt"));
		PrintWriter pw = new PrintWriter(fos, true);
		InetAddress address = Inet4Address.getLocalHost();
		int i = 1;
		while(i < 100){
			pw.println("" + i);
			System.out.println("pause : " + address.getHostAddress() + " :  " + i);
			//Thread.sleep(100000);
			i++;
		}
		pw.close();
		fs.close();
	}
}
