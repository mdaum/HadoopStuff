// map function for application to count the number of
// times each unique IP address 4-tuple appears in an
// adudump file.
import java.io.IOException;
import java.util.*;
import java.io.*;
import java.net.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.Mapper;

public class IpStatsMapper
  extends Mapper<LongWritable, Text, Text, ADURecord> {
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String line = value.toString();
    String[] tokens = line.split("\\s");
	if(!isClean(tokens))return; //i.e skip line
    String IPaddr1 = new String();
    String IPaddr2 = new String();
	String direction = tokens[3]; //capture direciton
    int last_dot;
	LongWritable bytes = new LongWritable(Long.parseLong(tokens[5])); //capture size of data
	// get the two IP address.port fields
        IPaddr1 = tokens[2];
	IPaddr2 = tokens[4];

	// eliminate the port part
	last_dot = IPaddr1.lastIndexOf('.');
	IPaddr1 = IPaddr1.substring(0, last_dot);
	last_dot = IPaddr2.lastIndexOf('.');
	IPaddr2 = IPaddr2.substring(0, last_dot);

        // output the key, value pairs where the key is an
        // IP address 4-tuple and the value is 1 (count)
        if(direction.equals(">")){ 
			context.write(new Text(IPaddr1), new ADURecord(new Text(IPaddr1),bytes,new LongWritable(0)));
			context.write(new Text(IPaddr2), new ADURecord(new Text(IPaddr2),new LongWritable(0),bytes));
		} //map sender determined by direction token
        else{
			context.write(new Text(IPaddr2), new ADURecord(new Text(IPaddr2),bytes,new LongWritable(0)));
			context.write(new Text(IPaddr1), new ADURecord(new Text(IPaddr1),new LongWritable(0),bytes));
		}
  }
  
	  public static boolean isClean(String[] tokens){
		  if(tokens.length<6)return false;
		  if(!tokens[0].contains("ADU"))return false;
		  if((tokens[2].split("\\.")).length!=5)return false;
		  if(tokens[4].split("\\.").length!=5)return false;
		  if(!tokens[3].equals("<")&&!tokens[3].equals(">"))return false;
		  try{
			  Integer.parseInt(tokens[5]);
		  }
		  catch(Exception e){
			  return false;
		  }
		  return true;
	  }
}

