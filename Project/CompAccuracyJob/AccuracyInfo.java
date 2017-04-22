import java.io.IOException;
import java.util.*;
import java.io.*;
import java.net.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
public class AccuracyInfo implements Writable{
	private DoubleWritable acc;
	private LongWritable numEntries;
	
	public AccuracyInfo(){
		this.acc=new DoubleWritable();
		this.numEntries = new LongWritable();
	}
	public AccuracyInfo(DoubleWritable a, LongWritable e){
		this.acc=a;
		this.numEntries=e;
	}
	
	@Override
	public void readFields(DataInput in)throws IOException{
		//ipAddr.readFields(in); //do the reads need to match up to the writes?
		acc.readFields(in);
		numEntries.readFields(in); //kept getting an EOFException here! Was due to 2 lines up..you must match what you write
	}
	
	@Override
	public void write(DataOutput out)throws IOException{ //don't need to output ipaddr...it is key
		acc.write(out);
		numEntries.write(out);
	}
	
	public DoubleWritable getAcc(){
		return acc;
	}
	public LongWritable getNumEntries(){
		return numEntries;
	}
	@Override
	public String toString(){
		double toRet =acc.get()/numEntries.get();
		
		return ""+toRet;
	}
}