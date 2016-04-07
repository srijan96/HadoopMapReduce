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

public class InverseLink {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{

    private Text src= new Text();
    private Text val = new Text();

    public void map(Object key, Text valueIn, Context context
                    ) throws IOException, InterruptedException {
      String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
      fileName = "http://" + fileName;
      fileName = fileName.replace(".html","");
      src.set(fileName);
      String value = valueIn.toString();
      value = value.replace(">.*?<","><");
      value = value.replace("</.*?>"," ");      
      String[] links = value.split(">");
      for(String i : links) {
      	if(i.matches(".*<a.*?http://.*?")){
      		i = i.replaceAll(".*<a.*?href.*?=","");
      		i = i.replace("\"","");
      		i = i.replace("\'","");  
      		val.set(i);
        	context.write(val,src);    		
      	}
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      String res = "";
      for (Text val : values) {
        res = res+val.toString();
      }
      result.set(res);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "inverse link index");
    job.setJarByClass(InverseLink.class);
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
