import java.util.StringTokenizer;
import java.util.*;
import java.io.*;

import org.apache.hadoop.fs.*;
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

public class MatrixVectorMultiplySplitted{

  public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{

    private IntWritable product = new IntWritable();
    private Text row = new Text();
    String line = "";
    
    public void setup(Context context) throws IOException
    {
    	String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
    	String ss = fileName.replace("matrix","vector");
    	ss = ss.replace("mat","vec");
    		      
        Path pt=new Path("hdfs:/user/srijan/input/"+ss);
        FileSystem fs = FileSystem.get(new Configuration());
        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
        line=br.readLine();
    }

    public void map(Object key, Text value, Context context ) throws IOException, InterruptedException 
    {
      String[] data = value.toString().split(",");
      Configuration conf = context.getConfiguration();
      String sW = conf.get("stripWidth");
      int stripWidth = Integer.parseInt(sW);
       if(data.length == 3 && !line.equals(""))
       {
	      	String[] vector = line.split(",");
	      	int col = Integer.parseInt(data[1]);
	      	int val = Integer.parseInt(data[2]);
	      	int m =  Integer.parseInt(vector[(col-1)%stripWidth]);
		row.set(data[0]);
		product.set(val * m);
		context.write(row,product);
       }
     }
  }

  public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("stripWidth","4");
    Job job = Job.getInstance(conf, "matrix vector multiply splitted");
    job.setJarByClass(MatrixVectorMultiplySplitted.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
