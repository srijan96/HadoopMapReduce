/***	CREATED BY GAURAV MITRA		***/

import java.io.IOException;
import java.util.*;
        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        
public class MatrixVector {
        
 public static class Map extends Mapper<Object, Text, IntWritable, Text> {
    
	private static Text sentence=new Text();
	private static IntWritable lineNumber=new IntWritable(0);
	
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        Random rand = new Random();
		int number=rand.nextInt(10)+1;
		sentence.set(line);
		context.write(new IntWritable(number),sentence);
		
    }
 } 
        
 public static class Reduce extends Reducer<IntWritable, Text, Text, Text> {
	private static Text out = new Text();
    public void reduce(IntWritable key, Iterable<Text> values, Context context) 
      throws IOException, InterruptedException 
	String pattern = "import";
	Pattern pat = Pattern.compile(pattern);
	Matcher match;
	String output="";
	for(Text val : values) {
		String line = val.toString();
		match = pat.matcher(line);
		if(match.find())  {
			output+=line+"\n";
		}
	}
	out.set(output);
	context.write(new Text(""), out);
    }
 }
        
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
        Job job = new Job(conf, "wordcount");
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
        
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    job.waitForCompletion(true);
 }
}