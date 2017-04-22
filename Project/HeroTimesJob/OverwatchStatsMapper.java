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
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class OverwatchStatsMapper
  extends Mapper<LongWritable, Text, Text, DoubleWritable> {
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String line = value.toString();
	line=Sanitize(line);//clean it up
	JSONParser j = new JSONParser();
	JSONObject top = null;
	try{
		top =(JSONObject)j.parse(line);
	}
	catch(ParseException e){
		return;//i.e skip line
	}
	ArrayList<JSONObject> regions = new ArrayList<JSONObject>();
	regions.add ((JSONObject)top.get("any"));//grab each region...guess the any region is else?
	regions.add((JSONObject)top.get("e"));//europe
	regions.add((JSONObject)top.get("kr"));//korea
	regions.add((JSONObject)top.get("us"));//usa
	for (JSONObject re : regions) {
	if(re==null)continue; //has not played in that region
	JSONObject times = (JSONObject)((JSONObject)((JSONObject)re.get("heroes")).get("playtime")).get("competitive");//grab playtime json obj
	if(times==null)continue;//malformed???
		for (Object o : times.entrySet()) {
			Map.Entry<String, Object> mapping = (Map.Entry<String, Object>)o;
			double time =(double)mapping.getValue();
			context.write(new Text(mapping.getKey()),new DoubleWritable(time));
		}
	}
  }
  
  public static String Sanitize(String line){
		line=line.replaceAll("u'", "\"");//lots of sanitizing
		line=line.replaceAll("'", "\"");
		line=line.replaceAll("None", "null");
		line=line.replaceAll("True", "true");
		line=line.replaceAll(": 0,", ": 0.0,");
		line=line.replaceAll(": 0}", ": 0.0}");
		line=line.toLowerCase();
		return line;
  }
	  
  
}

