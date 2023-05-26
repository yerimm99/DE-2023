import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class YouTubeStudent20190992
{
    public static class Emp{
        public String category;
        public double rating;

        public Emp(String _category, double _rating) {
            this.category = _category;
            this.rating = _rating;
        }
        public String getString() {
            return category + " " + rating;
        }

    }

    public static class YouTubeMapper1 extends Mapper<Text, Text, Text, DoubleWritable> {
        private Text one_key = new Text();
        private DoubleWritable one_value = new DoubleWritable();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), "|");
            itr.nextToken();
            itr.nextToken();
            itr.nextToken();
            String category = itr.nextToken().trim();
            itr.nextToken();
            itr.nextToken();
            double rating = Double.parseDouble(itr.nextToken().trim());

            one_key.set(category);
            one_value.set(rating);
            context.set(one_key, one_value);
        }
    }
    public static class YouTubeReducer1 extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values, Context
                context) throws IOException, InterruptedException {
            double agg_val = 0;
            double sum = 0;
            double cnt = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
                cnt++;
            }
            agg_val = (double) sum / cnt;
            result.set(agg_val);
            context.write(key, result);
        }

        public static class EmpComparator implements Comparator<Emp> {
            public int compare(Emp x, Emp y) {
                if (x.rating > y.rating) return 1;
                if (x.rating < y.rating) return -1;
                return 0;
            }
        }
    }
    public static void insertEmp(PriorityQueue q, String category, double rating, int topK) {
        Emp emp_head = (Emp) q.peek();
        if ( q.size() < topK || emp_head.rating < rating )
        {
            Emp emp = new Emp(category, rating);
            q.add( emp );
            if( q.size() > topK ) q.remove();
        }
    }

    public static class YouTubeMapper2 extends Mapper<Object, Text, Text, NullWritable> {
        private PriorityQueue<Emp> queue ;
        private Comparator<Emp> comp = new EmpComparator();
        private int topK;
        public void map(Object key, Text value, Context context) throws IOException,InterruptedException 			{
            StringTokenizer itr = new StringTokenizer(value.toString());
            String category = itr.nextToken().trim();
            double rating = Double.parseDouble(itr.nextToken().trim());

            insertEmp(queue, category, rating, topK);
        }
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            topK = conf.getInt("topK", -1);
            queue = new PriorityQueue<Emp>( topK , comp);
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            while( queue.size() != 0 ) {
                Emp emp = (Emp) queue.remove();
                context.write( new Text( emp.getString() ), NullWritable.get() );
            }
        }
    }
    public static class YouTubeReducer2 extends Reducer<Text, NullWritable, Text, NullWritable> {
        private PriorityQueue<Emp> queue ;
        private Comparator<Emp> comp = new EmpComparator();
        private int topK;
        public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException
        {
            StringTokenizer itr = new StringTokenizer(key.toString());
            String category = itr.nextToken().trim();
            double rating = Double.parseDouble(itr.nextToken().trim());

            insertEmp(queue, category, rating, topK);

        }

        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            topK = conf.getInt("topK", -1);
            queue = new PriorityQueue<Emp>( topK , comp);
        }
        protected void cleanup(Context context) throws IOException, InterruptedException {
            while( queue.size() != 0 ) {
                Emp emp = (Emp) queue.remove();
                context.write( new Text( emp.getString() ), NullWritable.get() );
            }
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        
        if (otherArgs.length != 3) {
            System.err.println("Usage: TopK <in> <out>");   System.exit(3);
        }
        conf.setInt("topK", Integer.parseInt(otherArgs[2]));
        String first_phase_result = "/first_phase_result";

        Job job1 = new Job(conf, "avgRating");
        job1.setJarByClass(YouTubeStudent20190992.class);
        job1.setMapperClass(YouTubeMapper1.class);
        job1.setReducerClass(YouTubeReducer1.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job1, new Path(first_phase_result));
        FileSystem.get(job1.getConfiguration()).delete(new Path(first_phase_result), true);
        System.exit(job1.waitForCompletion(true) ? 0 : 1);

        Job job2 = new Job(conf, "TopK");
        job2.setJarByClass(YouTubeStudent20190992.class);
        job2.setMapperClass(YouTubeMapper2.class);
        job2.setReducerClass(YouTubeReducer2.class);
        job2.setNumReduceTasks(1);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job2, new Path(first_phase_result));
        FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1]));
        FileSystem.get(job2.getConfiguration()).delete(new Path(otherArgs[1]), true);
        System.exit(job2.waitForCompletion(true) ? 0 : 1);

    }
}
