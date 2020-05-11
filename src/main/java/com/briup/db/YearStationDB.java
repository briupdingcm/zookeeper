package com.briup.db;



import org.apache.hadoop.mapreduce.lib.db.*;
import org.apache.hadoop.io.*;
import java.io.*;
import java.sql.*;

public class YearStationDB 
	implements DBWritable, WritableComparable{
	private YearStation  ysw;
	private int			temperature;

	public YearStationDB(){}

	public YearStationDB(YearStation ysw, 
			int temperature){
		this.ysw = ysw;
		this.temperature = temperature;
	}
	public int getTemperature(){return temperature;}

	public void setTemperature(int temperature){
		this.temperature = temperature;}

	public void readFields(ResultSet rs)
		throws SQLException{
		ysw.setYear(rs.getInt(1));
		ysw.setStationId(rs.getString(2));
		temperature = rs.getInt(3);
	}

	public void write(PreparedStatement ps)
		throws SQLException{
		ps.setInt(1, ysw.getYear());
		ps.setString(2, ysw.getStationId());
		ps.setInt(3, temperature);
	}

	public void readFields(DataInput input)
		throws IOException{
		ysw.readFields(input);
		temperature = input.readInt();
	}

	public void write(DataOutput output)
		throws IOException{
		ysw.write(output);
		output.writeInt(temperature);
	}

	public int compareTo(Object object){
		YearStationDB ysdb = (YearStationDB)object;
		return ysw.compareTo(ysdb.ysw);
	}
}
