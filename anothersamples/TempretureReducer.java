import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class TempretureReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
	
		 protected void reduce(final Text key, final Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException {
			    int sumOfTemperatures = 0;
			    int nbValues = 0;
			    for (IntWritable temperature : values) {
			      sumOfTemperatures += temperature.get();
			      nbValues++;
			    }
			    int average = sumOfTemperatures / nbValues;
			    context.write(key, new IntWritable(average));
			  }
}
