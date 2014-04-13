import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.naming.Context;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TempretureMapper extends Mapper<Text, Text, Text, IntWritable> {
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		System.out.println(key+"   "+value);
		if (isValueValid(value.toString())) {
			Text key2 = new Text(getStateFromValue(value.toString()));
			IntWritable value2 = new IntWritable(getTempretureFrom(value.toString()));
			context.write(key2, value2);
		}

	}

	
	private boolean isValueValid(final String value) {
		// We expect that the value is a String in the form of : State,
		// Temperature. E.g. MP,77
		Pattern p = Pattern.compile("\\S\\S\\,\\d+");
		Matcher m = p.matcher(value);
		return m.matches();

	}
	private String getStateFromValue(final String value){
		final String[] subvalues=value.split("\\,");
		return subvalues[0];
	}
	@SuppressWarnings("unused")
	private int getTempretureFrom(final String value){
		final String[] subvalues=value.split("\\,");
		return Integer.parseInt(subvalues[1]);
		
	}

}
