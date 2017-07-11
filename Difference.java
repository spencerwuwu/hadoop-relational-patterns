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


public class Difference {

	public static class MultipleMapA extends Mapper<LongWritable,Text,Text,IntWritable>
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

	public static class MultipleMapB extends Mapper<LongWritable,Text,Text,IntWritable>
	{

		private final static IntWritable Mone = new IntWritable(-1);
		private Text keyEmit = new Text();
		public void map(LongWritable k, Text value, Context context) throws IOException, InterruptedException
		{
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens())
			{
				keyEmit.set(itr.nextToken());
				context.write(keyEmit, Mone);
			}
		}
	}

	public static class MultipleReducer extends Reducer<Text,IntWritable,Text,IntWritable>
	{

		private IntWritable result = new IntWritable(1);

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException , InterruptedException
		{
            int sum = 0;
			for (IntWritable val : values) {
                if (val.get() < 0) {
                    sum = -1;
                }
                else sum = 0;
			}
            result.set(sum);
			context.write(key, result);
		}
	}

	public static class DifferenceMap extends Mapper<LongWritable,Text,Text,Text>
	{

		private Text keyEmit = new Text();
		private Text valEmit = new Text("");
		public void map(LongWritable k, Text value, Context context) throws IOException, InterruptedException
		{
            String line = value.toString();
            String[] words=line.split("\t");
            if (words[1].equals("0")) 
            {
				keyEmit.set(words[0]);
				context.write(keyEmit, valEmit);
            }
		}
	}

	public static class DifferenceReducer extends Reducer<Text,Text,Text,Text>
	{

		private IntWritable result = new IntWritable();

		public void reduce(Text key, Text values, Context context) throws IOException , InterruptedException
		{
			context.write(key, values);
		}
	}


	public static void main(String[] args) throws Exception
	{
		if (args.length != 4 ){
			System.err.println ("Usage :<inputlocation1> <inputlocation2> <outputlocation>");
			System.exit(0);
		}

		Configuration c=new Configuration();
		String[] files=new GenericOptionsParser(c,args).getRemainingArgs();
		Path p1=new Path(files[0]);
		Path p2=new Path(files[1]);
		Path p3=new Path(files[2]);
		Path p4=new Path(files[3]);
		FileSystem fs = FileSystem.get(c);
		if(fs.exists(p3)){
			fs.delete(p3, true);
		}
		if(fs.exists(p4)){
			fs.delete(p4, true);
		}
		Job job = Job.getInstance(c,"Difference");
		job.setJarByClass(Difference.class);
		MultipleInputs.addInputPath(job, p1, TextInputFormat.class, MultipleMapA.class);
		MultipleInputs.addInputPath(job,p2, TextInputFormat.class, MultipleMapB.class);
		job.setReducerClass(MultipleReducer.class);
		job.setCombinerClass(MultipleReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileOutputFormat.setOutputPath(job, p3);
        if (!job.waitForCompletion(true)) {
              System.exit(1);
        }


		Job job2 = Job.getInstance(c,"Difference");
		job2.setJarByClass(Difference.class);
		MultipleInputs.addInputPath(job2, p3, TextInputFormat.class, DifferenceMap.class);
		job2.setReducerClass(DifferenceReducer.class);
		job2.setCombinerClass(DifferenceReducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job2, p4);
        if (!job2.waitForCompletion(true)) {
              System.exit(1);
        }
	}
}
