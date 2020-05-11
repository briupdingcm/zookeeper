package com.briup.db;



import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import java.io.*;

public class YearStationRecordReader extends
		RecordReader<YearStation, IntWritable> {
	private NcdcRecordParser parser = new NcdcRecordParser();
	private LineRecordReader reader = new LineRecordReader();

	public void close() throws IOException {
		reader.close();
	}

	public YearStation getCurrentKey() throws IOException, InterruptedException {
		YearStation key = new YearStation(parser.getYear(),
				parser.getStationId());
		return key;
	}

	public IntWritable getCurrentValue() throws IOException,
			InterruptedException {
		IntWritable value = new IntWritable(parser.getTemperature());
		return value;
	}

	public float getProgress() throws IOException, InterruptedException {
		return reader.getProgress();
	}

	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		reader.initialize(split, context);
	}

	public boolean nextKeyValue() throws IOException {
		if (!reader.nextKeyValue())
			return false;
		Text value = reader.getCurrentValue();
		parser.parse(value);
		return parser.isValidTemperature();
	}

}
