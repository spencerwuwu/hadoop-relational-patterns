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

	public static class MultipleMapA extends Mapper<LongWritable,Text,Text,Text>
	{

		private Text keyEmit = new Text();
		private Text valEmit = new Text("A");
		public void map(LongWritable k, Text value, Context context) throws IOException, InterruptedException
		{
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens())
			{
				keyEmit.set(itr.nextToken());
				context.write(keyEmit, valEmit);
			}
		}
	}

	public static class MultipleMapB extends Mapper<LongWritable,Text,Text,Text>
	{

		private Text keyEmit = new Text();
		private Text valEmit = new Text("B");
		public void map(LongWritable k, Text value, Context context) throws IOException, InterruptedException
		{
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens())
			{
				keyEmit.set(itr.nextToken());
				context.write(keyEmit, valEmit);
			}
		}
	}

	public static class MultipleReducer extends Reducer<Text,Text,Text,Text>
	{

		private Text ValEmit = new Text("");

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException , InterruptedException
		{
            boolean flag = true;
			for (Text val : values) {
                if (val.toString().contains("B")) {
                    flag = false;
                    break;
                }
			}
            if (flag)
			    context.write(key, ValEmit);
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
		Job job = Job.getInstance(c,"Difference");
		job.setJarByClass(Difference.class);
		MultipleInputs.addInputPath(job, p1, TextInputFormat.class, MultipleMapA.class);
		MultipleInputs.addInputPath(job, p2, TextInputFormat.class, MultipleMapB.class);
		job.setReducerClass(MultipleReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, p3);
        if (!job.waitForCompletion(true)) {
              System.exit(1);
        }

	}
}
