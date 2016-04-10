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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MatrixMultiplySingleStep{

  public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{

    private Text pos = new Text();
    private IntWritable val = new IntWritable();

    public void map(Object key, Text value, Context context ) throws IOException, InterruptedException {
    
      Configuration conf = context.getConfiguration();
      int m= Integer.parseInt(conf.get("m"));
      int n = Integer.parseInt(conf.get("n"));
      int p = Integer.parseInt(conf.get("p"));        
 
      String[] data = value.toString().split(",");
      
      if(data.length == 4)
      {
       
       if(data[0].equals("A")){
      		val.set( Integer.parseInt(data[2])+ 10*Integer.parseInt(data[3]));
      		for(int i=1;i<=n;i++)
      		{
      			pos.set(data[1]+","+Integer.toString(i));
			context.write(pos,val);
		}
      	}
      	else if(data[0].equals("B")){
      		val.set( Integer.parseInt(data[1])+ 10*Integer.parseInt(data[3]));
      		for(int i=1;i<=n;i++)
      		{
      			pos.set(Integer.toString(i)+","+data[2]);
			context.write(pos,val);
		}
      	}
        
      }
    }
  }

  public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new  IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
    
     Configuration conf = context.getConfiguration();
      int n = 10;//Integer.parseInt(conf.get("n"));
      
      int sum = 0;
      int[] v1 = new int[n];  
      for(int i=0;i<n;i++)   	v1[i] = 0; 
      
      for (IntWritable val : values) {
	      int temp = val.get();
	      int pos = temp% 10;
	      if(pos == 0) pos = 10;
	      int V = temp / 10;
	      //sum = sum*100+V*10+pos;
	      	sum = sum*10 +V;
	     	v1[pos-1] = V;
	     	System.out.println("Hii"+V);
      }
      result.set(sum);
      context.write(key, result);
      
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("m","3");
    conf.set("n","3");
    conf.set("p","3");
    Job job = Job.getInstance(conf, "matrix vector multiply");
    job.setJarByClass(MatrixMultiplySingleStep.class);
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
