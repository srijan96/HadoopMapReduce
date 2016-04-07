import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DistGrep {

  public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>{

    private Text line = new Text();
    private Text fname = new Text();
    
    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_RED = "\u001B[31m";
    
    public void map(Object key, Text value, Context context)  throws IOException, InterruptedException 
    {    
	      Configuration conf = context.getConfiguration();
	      String obj = conf.get("pattern");
              String pattern = ".*?"+obj+".*?";
	      
	      String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
	      fname.set(fileName);
	      
	      String cur = value.toString();
	      if(cur.matches(pattern))
	      {
	        cur = cur.replace(obj,ANSI_RED+obj+ANSI_RESET);
		line.set(cur);
		context.write(fname, line);
	      }
    }
  }

  public static class IntSumReducer extends Reducer<Text,Text,Text,Text> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context )  throws IOException, InterruptedException 
    {
	      String res = "";
	      for (Text val : values) {
		res = res + val.toString() + "\n";
	      }
	      result.set(res);
	      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String pattern = "Hadoop";
    if(args.length > 2)	pattern = args[2];
    conf.set("pattern", pattern);
    Job job = Job.getInstance(conf, "distributed grep");
    job.setJarByClass(DistGrep.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
