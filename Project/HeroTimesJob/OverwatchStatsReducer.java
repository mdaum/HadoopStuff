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
  extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
  @Override
  public void reduce(Text key, Iterable<DoubleWritable> values,
      Context context)
      throws IOException, InterruptedException {

      double time=0;
      // iterate through all the records...adding up times played for each hero...
      for (DoubleWritable value : values) {
          time+=value.get();
      }
    context.write(key, new DoubleWritable(time));
  }
}
