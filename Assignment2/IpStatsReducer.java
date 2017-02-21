// reducer function for application to count the number of
// times each unique IP address 4-tuple appears in an
// adudump file.
import java.util.*;
import java.io.*;
import java.net.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.Reducer;

public class IpStatsReducer
  extends Reducer<Text, ADURecord, Text, ADURecord> {
  @Override
  public void reduce(Text key, Iterable<ADURecord> values,
      Context context)
      throws IOException, InterruptedException {

      int sent=0;
	  int rec=0;
      // iterate through all the records...adding it to sent and receive...output new ADURecord which is writeable
      for (ADURecord value : values) {
          sent+=value.getSent().get();
		  rec+=value.getRecieved().get();
      }
    context.write(key, new ADURecord(key,new LongWritable(sent),new LongWritable(rec)));
  }
}
