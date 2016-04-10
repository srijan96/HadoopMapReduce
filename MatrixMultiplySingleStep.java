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

  public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>{

    private Text pos = new Text();
    private Text val = new Text();

    public void map(Object key, Text value, Context context ) throws IOException, InterruptedException {
    
      Configuration conf = context.getConfiguration();
      int m= Integer.parseInt(conf.get("m"));
      int n = Integer.parseInt(conf.get("n"));
      int p = Integer.parseInt(conf.get("p"));        
 
      String[] data = value.toString().split(",");
      
      if(data.length == 4)
      {
       
       if(data[0].equals("A")){
      		val.set(data[2]+ ","+data[3]);
      		for(int i=1;i<=n;i++)
      		{
      			pos.set(data[1]+","+Integer.toString(i));
			context.write(pos,val);
		}
      	}
      	else if(data[0].equals("B")){
      		val.set( data[1]+ ","+data[3]);
      		for(int i=1;i<=n;i++)
      		{
      			pos.set(Integer.toString(i)+","+data[2]);
			context.write(pos,val);
		}
      	}
        
      }
    }
  }

  public static class IntSumReducer extends Reducer<Text,Text,Text,Text> {
    private Text result = new  Text();

    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
    
     Configuration conf = context.getConfiguration();
      int n = Integer.parseInt(conf.get("n"));
      
      int sum = 0;
      int[] v1 = new int[n];  
      for(int i=0;i<n;i++)   	v1[i] = 0; 
      
      for (Text val : values) {
      	      String[] ss = val.toString().split(",");
	      int pos = Integer.parseInt(ss[0]);
	      int V = Integer.parseInt(ss[1]);
	      sum = sum +V*v1[pos-1];
	      v1[pos-1] = V;
      }
      result.set(Integer.toString(sum));
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
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
