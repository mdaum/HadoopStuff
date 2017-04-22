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
  extends Mapper<LongWritable, Text, Text, AccuracyInfo> {
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
	JSONObject stats = (JSONObject)((JSONObject)((JSONObject)re.get("heroes")).get("stats")).get("competitive");//grab comp stats json obj
		if(stats==null)continue;//malformed?
		for(Object o : stats.entrySet()){
			Map.Entry<String, Object>mapping = (Map.Entry<String,Object>)o;
			try{
				double time=(double)((JSONObject)((JSONObject)mapping.getValue()).get("general_stats")).get("time_played");
				double acc =(double)((JSONObject)((JSONObject)mapping.getValue()).get("general_stats")).get("weapon_accuracy");
				if(time>1)context.write(new Text(mapping.getKey()),new AccuracyInfo(new DoubleWritable(acc),new LongWritable(1)));
			}
			catch(NullPointerException e){
				continue;//skip hero...doesn't have weapon acc
			}
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
		line=line.replaceAll(": \"--\"",": 0.0");
		line=line.toLowerCase();
		return line;
  }
	  
  
}

