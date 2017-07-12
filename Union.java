import java.io.*;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Union {

	public static class MultipleMap extends Mapper<LongWritable,Text,Text,IntWritable>
	{

		private final static IntWritable one = new IntWritable(1);
		private Text keyEmit = new Text();
		public void map(LongWritable k, Text value, Context context) throws IOException, InterruptedException
		{
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens())
			{
				keyEmit.set(itr.nextToken());
				context.write(keyEmit, one);
			}
		}
	}


	public static class MultipleReducer extends Reducer<Text,IntWritable,Text,IntWritable>
	{

		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException , InterruptedException
		{
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception
	{
		if (args.length != 3 ){
			System.err.println ("Usage :<inputlocation1> <inputlocation2> <outputlocation>");
			System.exit(0);
		}

		Configuration c=new Configuration();
		String[] files=new GenericOptionsParser(c,args).getRemainingArgs();
		Path p1=new Path(files[0]);
		Path p2=new Path(files[1]);
		Path p3=new Path(files[2]);
		FileSystem fs = FileSystem.get(c);
		if(fs.exists(p3)){
			fs.delete(p3, true);
		}
		Job job = Job.getInstance(c,"Union");
		job.setJarByClass(Union.class);
		MultipleInputs.addInputPath(job, p1, TextInputFormat.class, MultipleMap.class);
		MultipleInputs.addInputPath(job,p2, TextInputFormat.class, MultipleMap.class);
		job.setReducerClass(MultipleReducer.class);
		job.setCombinerClass(MultipleReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileOutputFormat.setOutputPath(job, p3);
		boolean success = job.waitForCompletion(true);
		System.exit(success?0:1);
	}
}
