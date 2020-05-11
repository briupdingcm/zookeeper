package com.briup.db;



import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.*;
import org.apache.hadoop.io.*;
import java.io.*;

public class YearStationInputFormat extends
		FileInputFormat<YearStation, IntWritable> {
	
	public RecordReader<YearStation, IntWritable> 
	createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		YearStationRecordReader ysrr = 
				new YearStationRecordReader();
		ysrr.initialize(split, context);
		return ysrr;
	}
}
