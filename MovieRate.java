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

public class MovieRate {

  public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>{

	public static final String[] GENRES = {"Action","Adventure","Animation","Children","Comedy","Crime","Documentary","Drama","Fantasy","Film-Noir","Horror","Musical","Mystery","Romance","Sci-Fi","Thriller","War","Western"};
	
	private Text keyOut = new Text();
	private Text valOut = new Text();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
	{
		String[] in = value.toString().split(",");
		String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
		if(fileName.equals("ratings.csv") && in.length >= 3)
		{
			String sKey = in[1];
			String sVal = in[0]+","+in[2];
			keyOut.set(sKey);
			valOut.set(sVal);
			context.write(keyOut,valOut);
		}
		else if(fileName.equals("movies.csv") && in.length >= 3)
		{
			String sKey = in[0];
			String sVal = "";
			int i=0,j=0;
			String[] genres = in[2].split("\\|");
			if(genres[0].contains("no genres listed"))
			{
				sVal = "0";
				for(i=0;i<GENRES.length-1;i++)		sVal = sVal+",0";
				keyOut.set(sKey);
				valOut.set(sVal);
				context.write(keyOut,valOut);
			}
			else if(genres.length > 0)
			{
				sVal = "";
				int[] arr = {0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0};
				for(j=0;j<genres.length;j++)
				{
					for(i=0;i<GENRES.length;i++)
					{
						if(genres[j].contains(GENRES[i]))
							arr[i] = 1;
					}
				}
				sVal = Integer.toString(arr[0]);
				for(i=1;i<GENRES.length;i++)		sVal = sVal+","+Integer.toString(arr[i]);
				keyOut.set(sKey);
				valOut.set(sVal);
				context.write(keyOut,valOut);
			}
		}
	}
  }

  public static class IntSumReducer extends Reducer<Text,Text,Text,Text> {
  
	private Text res = new Text();
	private Text keyOut = new Text();
	public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException 
	{
		String gen  = "";
		String g1 = "";
		ArrayList<String> cache = new ArrayList<String>();		
		for (Text val : values) 
		{
			if(val.toString().split(",").length >= 10)
			{
				gen = val.toString();
			}
			else
			{
				cache.add(val.toString());
			}
		}
		for (String val2 : cache) 
		{
			if(!gen.equals(""))
			{
				String[] r = val2.split(",");
				g1 = gen.replace("1",r[1]);
				res.set(g1);
				keyOut.set(r[0]);
				context.write(keyOut, res);
			}
		}
	}
  }

  public static void main(String[] args) throws Exception 
  {
	Configuration conf = new Configuration();
	Job job = Job.getInstance(conf, "movie rate");
	job.setJarByClass(MovieRate.class);
	job.setMapperClass(TokenizerMapper.class);
	job.setReducerClass(IntSumReducer.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
