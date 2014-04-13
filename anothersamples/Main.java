import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class Main {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf=new Configuration();
		String[] otherArgs= new GenericOptionsParser(conf,args).getRemainingArgs();
		if(otherArgs.length!=2){
			System.err.println("Usage :Main <in> <out>");
			System.exit(-1);
		}
		
		File file1 = new File(args[1]);
		if(file1.exists())
		FileUtils.deleteDirectory(file1);
	
		Job job=new Job(conf,"Ortalama Sicakligi hesapla");
		//text dosyasında okudugunun bildirgesi
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		job.setJarByClass(Main.class);
		
		job.setMapperClass(TempretureMapper.class);
		job.setReducerClass(TempretureReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		System.exit(job.waitForCompletion(true) ? 0 :-1);
		
	}

}
