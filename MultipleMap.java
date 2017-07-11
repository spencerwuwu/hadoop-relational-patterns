import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MultipleMap1 extends Mapper<LongWritable,Text,Text,IntWritable>
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
