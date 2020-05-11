package com.briup.db;



import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;

public class MaxTemperatureByYearStation {
	static class MaxTemperatureMapper extends
			Mapper<YearStation, IntWritable, YearStation, IntWritable> {
		public void map(YearStation key, IntWritable value, Context context)
				throws IOException, InterruptedException {
			context.write(key, value);
		}
	}

	static class MaxTemperatureReducer extends
			Reducer<YearStation, IntWritable, YearStationDB, NullWritable> {
		public void reduce(YearStation key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int maxValue = Integer.MIN_VALUE;
			for (IntWritable value : values) {
				maxValue = Math.max(maxValue, value.get());
			}
			context.write(new YearStationDB(key, maxValue), NullWritable.get());
		}
	}

	public static boolean run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(MaxTemperatureByYearStation.class);
		job.setJobName("max temperature by year station");

		YearStationInputFormat.addInputPath(job, new Path(args[0]));

		DBConfiguration.configureDB(job.getConfiguration(),
				"com.mysql.jdbc.Driver", 
				"jdbc:mysql://server0.cloud.briup.com:3306/zookeeper",
				"zookeeper", "zookeeper");
		DBOutputFormat.setOutput(job, "dingcm_temp", "year", "station",
				"temperature");

		job.setMapperClass(MaxTemperatureByYearStation.MaxTemperatureMapper.class);
		job.setMapOutputKeyClass(YearStation.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setReducerClass(MaxTemperatureByYearStation.MaxTemperatureReducer.class);
		job.setOutputKeyClass(YearStationDB.class);
		job.setOutputValueClass(NullWritable.class);

		job.setInputFormatClass(YearStationInputFormat.class);
		job.setOutputFormatClass(DBOutputFormat.class);

		return job.waitForCompletion(true);
	}

	public static void main(String... args) throws Exception {
		System.exit(run(args) ? 0 : 1);
	}
}
