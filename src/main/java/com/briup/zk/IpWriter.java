package com.briup.zk;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.briup.zk.IpWriter.IpMapper.IpReducer;

public class IpWriter extends Configured implements Tool{
	static class IpMapper extends Mapper<LongWritable, Text, Text, Text>{
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			InetAddress address = Inet4Address.getLocalHost();
			Text ip =  new Text(address.getHostAddress());
			long id = Thread.currentThread().getId();
			context.write(ip, ip);
		}
		static class IpReducer extends Reducer<Text,Text,Text,Text>{

			@Override
			protected void reduce(Text key, Iterable<Text> values,
					Reducer<Text, Text, Text, Text>.Context context)
					throws IOException, InterruptedException {
				// TODO Auto-generated method stub
					context.write(key, values.iterator().next());
				
			}
			
		}
	}
	public static void main(String... args) throws Exception{
		Configuration conf = new Configuration();
	//	conf.set("yarn.resourcemanager.address", "server1.cloud.briup.com:18040");
	//	conf.set("yarn.resourcemanager.scheduler.address", "server1.cloud.briup.com:18030");
	//	conf.set("fs.defaultFS", "hdfs://server1.cloud.briup.com:9000");

	//	conf.get()
	//	conf.setInt("mapred.map.tasks", 3);
		ToolRunner.run(conf, new IpWriter(), args);
	}

	@Override
	public int run(String[] args0) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf);
		job.setJarByClass(IpWriter.class);
		job.setJobName("IP Writer");
		job.setNumReduceTasks(3);
		
		
		job.setMapperClass(IpMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(IpReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args0[0]));
		FileOutputFormat.setOutputPath(job, new Path(args0[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}
}
