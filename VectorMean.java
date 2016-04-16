import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class VectorMean {

  public static class TokenizerMapper extends Mapper<Object, Text, IntWritable, Text>{
	
	private IntWritable keyOut = new IntWritable();
	private Text valOut = new Text();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
	{
		String[] in = value.toString().split("\\s");
		if(in.length >= 2)
		{
			keyOut.set(Integer.parseInt(in[0]));
			valOut.set(in[1]);
			context.write(keyOut,valOut);
		}
	}
		
  }

  public static class IntSumReducer extends Reducer<IntWritable,Text,IntWritable,Text> {
  
	private Text res = new Text();
	public void reduce(IntWritable key, Iterable<Text> values,Context context) throws IOException, InterruptedException 
	{
		float[] arr = {0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0};
		int count = 0;
		for(Text val:values)
		{
			String[] l = val.toString().split(",");
			for(int i=0;i<l.length && i<18;i++)
				arr[i] = arr[i]+Float.parseFloat(l[i]);
			count = count + 1;
		}
		if(count > 0)
		{
			String s = Float.toString(arr[0]/count);
			for(int i=1; i<18;i++)		s = s+","+Float.toString(arr[i]/count);
			res.set(s);
			context.write(key,res);
		}
	}
  }

  public static void main(String[] args) throws Exception 
  {
	Configuration conf = new Configuration();
	Job job = Job.getInstance(conf, "Vector Mean");
	job.setJarByClass(VectorMean.class);
	job.setMapperClass(TokenizerMapper.class);
	job.setReducerClass(IntSumReducer.class);
	job.setOutputKeyClass(IntWritable.class);
	job.setOutputValueClass(Text.class);
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
