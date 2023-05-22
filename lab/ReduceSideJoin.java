import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapred.lib.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class ReduceSideJoin
{
	public static class ReduceSideJoinMapper extends Mapper<Object, Text, Text, Text>
	{
		boolean fileA = true;
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			StringTokenizer itr = new StringTokenizer(value.toString(), "|");
			Text outputKey = new Text();
			Text outputValue = new Text();
			String joinKey = "";
			String o_value = "";
			if( fileA ) {
				String id = itr.nextToken();
				String price = itr.nextToken();
				joinKey = itr.nextToken();
				o_value = "A," + id +"," + price; 
			}
			else  {
				joinKey = itr.nextToken();
				String description = itr.nextToken();
				o_value = "B," + description;
			}
			outputKey.set( joinKey );
			outputValue.set( o_value );
			context.write( outputKey, outputValue );
		}
		protected void setup(Context context) throws IOException, InterruptedException
		{
			String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
			if ( filename.indexOf( "relation_a" ) != -1 ) fileA = true;
			else fileA = false;
		}
	}

	public static class ReduceSideJoinReducer extends Reducer<Text,Text,Text,Text>
	{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException  {
			Text reduce_key = new Text();
			Text reduce_result = new Text();
			String description = "";
			ArrayList<String> buffer = new ArrayList<String>();
			for (Text val : values) {
				String file_type;
				file_type = val.toString();
				if( file_type.equals( "B" ) )  {
					description = val.toString();
				}
				else  {
					if ( description == "" ) {
						buffer.add( val.toString() );
					}
					else {
						buffer.add( val.toString() );
						context.write(reduce_key, reduce_result);
					}
				}
			}

		}
	}
	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		int topK = 3;
		if (otherArgs.length != 2)
		{
			System.err.println("Usage: ReduceSideJoin <in> <out>");
			System.exit(2);
		}
		conf.setInt("topK", topK);
		Job job = new Job(conf, "ReduceSideJoin");
		job.setJarByClass(ReduceSideJoin.class);
		job.setMapperClass(ReduceSideJoinMapper.class);
		job.setReducerClass(ReduceSideJoinReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
