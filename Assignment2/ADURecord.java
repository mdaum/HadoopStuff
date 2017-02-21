import java.io.IOException;
import java.util.*;
import java.io.*;
import java.net.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
public class ADURecord implements Writable{
	private Text ipAddr; //for debugging purposes really
	private LongWritable Sent;
	private LongWritable Recieved;
	
	public ADURecord(Text ip,LongWritable sent, LongWritable rec){
		this.ipAddr=ip;
		this.Sent=sent;
		this.Recieved=rec;
	}
	
	@Override
	public void readFields(DataInput in)throws IOException{
		ipAddr.readFields(in);
		Sent.readFields(in);
		Recieved.readFields(in);
	}
	
	@Override
	public void write(DataOutput out)throws IOException{ //don't need to output ipaddr...it is key
		Sent.write(out);
		Recieved.write(out);
	}
	
	public Text getIp(){
		return ipAddr;
	}
	public LongWritable getSent(){
		return Sent;
	}
	public LongWritable getRecieved(){
		return Recieved;
	}
}