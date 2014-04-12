package com.sertac.example;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PracticeMapper extends
    Mapper<LongWritable, Text, Text, IntWritable> {
  public void map(LongWritable key, Text value, Context context) {
    String line = value.toString();
    String[] fields = line.split("~");
    String head = fields[0];
    int rate = Integer.valueOf(fields[4]);
    try {
      context.write(new Text(head), new IntWritable(rate));
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

}
