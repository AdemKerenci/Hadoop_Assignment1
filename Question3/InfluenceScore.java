package org.mapreduce.influencescore;
        
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
        

public class InfluenceScore  {
 
 public static class UserTupleWritable implements Writable{
    private Text user1;
    private Text user2;

    public UserTupleWritable() {
        this.user1 = new Text();
        this.user2 = new Text();
    }

    public UserTupleWritable(Text user1, Text user2) {
	this.user1 = user1;
	this.user2 = user2;
    }

    public void readFields(DataInput in) throws IOException {
        user1.readFields(in);
        user2.readFields(in);
    }

    public void write(DataOutput out) throws IOException {
        user1.write(out);
	user2.write(out);
    }

    public void set(Text user1, Text user2) {
    	this.user1 = user1;
        this.user2 = user2;
    }

    public String getUser1() {
	return this.user1.toString();
    }

   public String getUser2() {
  	return this.user2.toString();
    }
   
 }
 
 public static class KeyUserTupleWritable implements WritableComparable<KeyUserTupleWritable> {
    private Text followedUser;
    private UserTupleWritable userTuple;

    public KeyUserTupleWritable() {
    	this.followedUser = new Text();
        this.userTuple = new UserTupleWritable();
    }
                  
    public KeyUserTupleWritable(Text followed, UserTupleWritable tuple) {
        this.followedUser = followed;
 	this.userTuple = tuple;
    }    

    public void set(Text followed, UserTupleWritable tuple) {
        this.followedUser = followed;
 	this.userTuple = tuple;
    }
    
    public void readFields(DataInput in) throws IOException {
        followedUser.readFields(in);
        userTuple.readFields(in);
    }

    public void write(DataOutput out) throws IOException {
        followedUser.write(out);
	userTuple.write(out);
    }

    public String getFollowedUser() {
   	return this.followedUser.toString();
    }

    public int compareTo(KeyUserTupleWritable other) {
	String thisUser1 = this.userTuple.getUser1();
	String thisUser2 = this.userTuple.getUser2();
	String otherUser1 = other.userTuple.getUser1();
	String otherUser2 = other.userTuple.getUser2();
	
	if (thisUser1.equals(otherUser1) && thisUser2.equals(otherUser2)) 
	    return 0;

        String otherFollowedUser = other.getFollowedUser();
	return getFollowedUser().compareTo(otherFollowedUser);

    }

    public String toString() {
	return getFollowedUser();
    }
 }

 public static class ValueUserScoreTupleWritable implements Writable{
     private Text followingUser;
     private IntWritable score;

     public ValueUserScoreTupleWritable() {
	this.followingUser = new Text();
	this.score = new IntWritable();
     }
     
     public ValueUserScoreTupleWritable(Text followingUser, IntWritable score) {
	this.followingUser = followingUser;
	this.score = score;
     }
    
     public void set(Text followingUser, IntWritable score) {
	this.followingUser = followingUser;
	this.score = score;
     }

     public void readFields(DataInput in) throws IOException {
        followingUser.readFields(in);
        score.readFields(in);
    }

    public void write(DataOutput out) throws IOException {
        followingUser.write(out);
	score.write(out);
    }

    public String getFollowingUser() {
	return this.followingUser.toString();
    }

    public int getScore() {
        return this.score.get();
    }

    public String toString() {
 	return Integer.toString(this.score.get());
    }
 }

        
 public static class Map extends Mapper<LongWritable, Text, KeyUserTupleWritable, ValueUserScoreTupleWritable > {
    private final static IntWritable one = new IntWritable(1);
    private Text user1 = new Text();
    private Text user2 = new Text();
    private UserTupleWritable userTuple = new UserTupleWritable();
    private KeyUserTupleWritable keyOfPair = new KeyUserTupleWritable();
    private ValueUserScoreTupleWritable valueOfPair = new ValueUserScoreTupleWritable();
    private Text followedUser = new Text();
        
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String [] tuple = line.split("    ");

        valueOfPair.set(new Text(tuple[0]),one);
	followedUser.set(tuple[1]);

        Arrays.sort(tuple);

        user1.set(tuple[0]);
	user2.set(tuple[1]);
	userTuple.set(user1,user2);
	keyOfPair.set(followedUser,userTuple);
     
        context.write(keyOfPair,valueOfPair);
    }
 }
        
 public static class Reduce extends Reducer<KeyUserTupleWritable, ValueUserScoreTupleWritable,KeyUserTupleWritable, ValueUserScoreTupleWritable> {
    private Text followedUser = new Text();
    private IntWritable score = new IntWritable();
    private ValueUserScoreTupleWritable outValue = new ValueUserScoreTupleWritable();

    public void reduce(KeyUserTupleWritable key, Iterable<ValueUserScoreTupleWritable> values, Context context) 
      throws IOException, InterruptedException {
        int scoreInt = 0;
	String user1 = key.userTuple.getUser1();
	String user2 = key.userTuple.getUser2();
	boolean isUser1In = false;
	boolean isUser2In = false;	
        for (ValueUserScoreTupleWritable usr : values) {
            String followingUser = usr.getFollowingUser();
            if (followingUser.equals(user1))
		isUser1In = true; 
	    else if (followingUser.equals(user2)) 
		isUser2In = true;

	    scoreInt += usr.getScore();
        }
 	
	if (isUser1In && isUser2In)
	    scoreInt -= 2;

      	score.set(scoreInt);
	outValue.set(new Text(""),score);
        context.write(key,outValue);
    }
 }
        
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
    Job job = new Job(conf, "influencescore");
    
    job.setJarByClass(InfluenceScore.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setCombinerClass(Reduce.class);   
 
    job.setOutputKeyClass(KeyUserTupleWritable.class);
    job.setOutputValueClass(ValueUserScoreTupleWritable.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    
    job.setNumReduceTasks(3);    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    System.exit(job.waitForCompletion(true) ? 0 : 1);
 }
        
}
