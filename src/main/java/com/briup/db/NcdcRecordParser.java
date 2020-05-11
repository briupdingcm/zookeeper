package com.briup.db;



import org.apache.hadoop.io.*;

public class NcdcRecordParser{
	private static final int MISSING = 9999;
	private String	stationId;
	private int		year;
	private int		temperature;
	private boolean isValidTemperature;

	public NcdcRecordParser(){}

	public void parse(String line){
		if(line.length() < 93){
			isValidTemperature = false;
			return;
		}
		stationId = line.substring(0, 15);
		year = Integer.parseInt(line.substring(15, 19));
		if(line.charAt(87) == '+'){
			temperature = Integer.parseInt((line.substring(88, 92)));
		}else{
			temperature = Integer.parseInt((line.substring(87, 92)));
		}
		String quality = line.substring(92, 93);
		if((temperature != MISSING) && (quality.matches("[01459]")))
			isValidTemperature = true;
		else
			isValidTemperature = false;
	}
	public String getStationId() {
		return stationId;
	}

	public void setStationId(String stationId) {
		this.stationId = stationId;
	}

	public void parse(Text value){
		parse(value.toString());
	}

	public int getYear(){return year;}

	public void setYear(int year){this.year = year;}

	public int getTemperature(){return temperature;}

	public void setTemperature(int temperature){
		this.temperature = temperature;
	}

	public void setValidTemperature(boolean isValidTemperature) {
		this.isValidTemperature = isValidTemperature;
	}
	public boolean isValidTemperature(){return isValidTemperature;}
}
