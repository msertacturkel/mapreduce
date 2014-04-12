package com.sertac.example;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PractiseMaxValueJob {
  public static void main(String[] args){
    if (args.length != 2) {
      System.err
          .println("Usage PracticeMaxValueJob <input path> <output path>");
      System.exit(1);
    }
    try {
      Job job=new Job();
      job.setJarByClass(PractiseMaxValueJob.class);
      job.setJobName("Practice MaxValueJob");
      FileInputFormat.addInputPath(job, new Path(args[0]));
      SimpleDateFormat sdf=new SimpleDateFormat("MM_dd_HH_mm_ss");
      sdf.format(new Date(System.currentTimeMillis()));
      FileOutputFormat.setOutputPath(job, new Path(args[1] + "/" + sdf.toPattern()));
      job.setMapperClass(PracticeMapper.class);
      job.setReducerClass(PracticeReducer.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);
      System.exit(job.waitForCompletion(true)?0:1);
      
    } catch (IOException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

}
