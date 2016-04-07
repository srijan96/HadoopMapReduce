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
//    private final static IntWritable one = new IntWritable(1);
 //   private Text word = new Text();
    private static IntWritable number = new IntWritable(0);
    private Text word = new Text();
        
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        /*StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken());
            context.write(word, one);
        }*/
	while(line != null) {
		String array[]=line.split(" ");
		int mykey = Integer.parseInt (array[0]);
		String currentValue = (array[2]+" "+array[1]);
		word.set(currentValue);
		context.write(new IntWritable(mykey),word);
	}
    }
 } 
        
 public static class Reduce extends Reducer<IntWritable, Text, IntWritable, IntWritable> {

    public void reduce(IntWritable key, Iterable<Text> values, Context context) 
      throws IOException, InterruptedException 
	int arr[]={1,2,3,4,5};
        /*int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        context.write(key, new IntWritable(sum));*/
	int sum=0;
	for(Text val : values) {
		String value = val.toString();
		String array[]=value.split("");
		int column = Integer.parseInt(array[0]);
		int element = Integer.parseInt(array[1]);
		sum+=(arr[column]*element);
	}
	context.write(key, new IntWritable(sum));
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
