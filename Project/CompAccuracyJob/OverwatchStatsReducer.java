// reducer function for application to count the number of
// times each unique IP address 4-tuple appears in an
// adudump file.
import java.util.*;
import java.io.*;
import java.net.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.Reducer;

public class OverwatchStatsReducer
  extends Reducer<Text, AccuracyInfo, Text, AccuracyInfo> {
  @Override
  public void reduce(Text key, Iterable<AccuracyInfo> values,
      Context context)
      throws IOException, InterruptedException {
		double acc=0;
		long num=0;
      // iterate through all the records...adding up times played for each hero...
      for (AccuracyInfo value : values) {
          acc+=value.getAcc().get();
		  num+=value.getNumEntries().get();
      }
    context.write(key, new AccuracyInfo(new DoubleWritable(acc),new LongWritable(num)));
  }
}
