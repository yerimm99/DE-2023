import java.io.*;
import java.lang.module.Configuration;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.FSDataInputStream;
import org.w3c.dom.Text;

import javax.naming.Context;

public class IMDBStudent20190992
{
    public static class IMDBMapper extends Mapper<Object, Text, Text, IntWritable>
    {
        private String filename;
        private Text genre = new Text();
        private IntWritable cnt = new IntWritable();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), "::");
            String num = itr.nextToken();
            String title = itr.nextToken();
            String list = itr.nextToken();

            StringTokenizer itr2 = new StringTokenizer(list, "|");
            while(itr2.hasMoreTokens()){
                String token = itr2.nextToken();
                genre.set(token);
                cnt.set(1);
                context.write(genre, cnt);
            }

        }
        protected void setup(Context context) throws IOException, InterruptedException
        {
            filename = ((FileSplit)context.getInputSplit()).getPath().getName();
        }
    }

    public static class IMDBReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
        private IntWritable result = new IntWritable();
        Text genre = new Text();
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values)
                sum += val.get();
            genre.set(key.toString());
            result.set(sum);
            context.write(genre, result);
        }
    }




    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "IMDB");

        job.setJarByClass(IMDBStudent20190992.class);
        job.setMapperClass(IMDBMapper.class);
        job.setReducerClass(IMDBReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class)

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        FileSystem.get(job.getConfiguration()).delete( args[1], true);
        job.waitForCompletion(true);


    }
}
