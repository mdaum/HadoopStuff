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
  extends Reducer<Text, LongWritable, Text, LongWritable> {
  @Override
  public void reduce(Text key, Iterable<LongWritable> values,
      Context context)
      throws IOException, InterruptedException {

      long count=0;
      // iterate through all the records...adding up times played for each hero...
      for (LongWritable value : values) {
          count+=value.get();
      }
    context.write(key, new LongWritable(count));
  }
}
