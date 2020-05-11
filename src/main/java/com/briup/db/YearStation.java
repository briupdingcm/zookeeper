package com.briup.db;



import org.apache.hadoop.io.*;
import java.io.*;

public class YearStation 
	implements WritableComparable<YearStation>{
	private int year;
	private String stationId;

	public YearStation(){}

	public YearStation(int year, String sId){
		this.year = year;
		this.stationId = sId;
	}

	public void readFields(DataInput input)
		throws IOException{
		year = input.readInt();
		stationId = input.readUTF();
	}

	public void write(DataOutput output)throws IOException{
		output.writeInt(year);
		output.writeUTF(stationId);
	}

	public int compareTo(YearStation ys){
		return (year != ys.year)?
			((year > ys.year)?1:-1)
			:(stationId.compareTo(ys.stationId));
	}

	public int hashCode(){
		return year + stationId.hashCode();
	}

	public String toString(){
		return year + ":" + stationId;
	}

	public void setYear(int year){this.year = year;}

	public void setStationId(String sId){
		this.stationId = sId;
	}

	public int getYear(){return year;}

	public String getStationId(){return stationId;}

}


