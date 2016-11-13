package org.mapreduce.groupavg;
        
import java.io.IOException;
import java.util.*;
import java.io.DataInput;
import java.io.DataOutput;
        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        

public class GroupAvg  {
 
 public static class AverageCountWritable implements Writable{
    private DoubleWritable average;
    private IntWritable count;

    public AverageCountWritable() {
        this.average = new DoubleWritable();
        this.count = new IntWritable();
    }

    public AverageCountWritable(DoubleWritable average, IntWritable count) {
	this.average = average;
	this.count = count;
    }

    public void readFields(DataInput in) throws IOException {
        average.readFields(in);
        count.readFields(in);
    }

    public void write(DataOutput out) throws IOException {
        average.write(out);
	count.write(out);
    }

    public void set(DoubleWritable average, IntWritable count) {
    	this.average = average;
        this.count = count;
    }

    public double getAverage() {
	return this.average.get();
    }

   public  int getCount() {
  	return this.count.get();
    }
   
   public String toString() {
        return Double.toString(getAverage());
   }
 }
        
 public static class Map extends Mapper<LongWritable, Text, Text, AverageCountWritable> {
    private final static IntWritable one = new IntWritable(1);
    private AverageCountWritable averageIncomeCount = new AverageCountWritable();
    private Text cityName = new Text();
        
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String [] tuple = line.split("   ");
        averageIncomeCount.set(new DoubleWritable(Double.parseDouble(tuple[1])),one);
        cityName.set(tuple[0]);
        context.write(cityName,averageIncomeCount);
    }
 }
        
 public static class Reduce extends Reducer<Text, AverageCountWritable, Text, AverageCountWritable> {

    public void reduce(Text key, Iterable<AverageCountWritable> values, Context context) 
      throws IOException, InterruptedException {
        double sum = 0.0;
        int counter = 0;
        for (AverageCountWritable val : values) {
            sum = sum + val.getAverage();
            counter = counter + val.getCount();
        }
        double avg = sum / counter;
        context.write(key, new AverageCountWritable(new DoubleWritable(avg),new IntWritable(counter)));
    }
 }
        
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
    Job job = new Job(conf, "groupavg");
    
    job.setJarByClass(GroupAvg.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setCombinerClass(Reduce.class);   
 
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(AverageCountWritable.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    
    job.setNumReduceTasks(4);    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    System.exit(job.waitForCompletion(true) ? 0 : 1);
 }
        
}
