import java.io.IOException;
import java.util.*;
import java.io.InputStream;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

class Emp{
        public String category;
        public double rating;

        public Emp(String _category, double _rating) {
            this.category = _category;
            this.rating = _rating;
        }
        public String getCategory() {
            return category;
        }
        public double getRating() {
            return rating;
        }

    }

public class YouTubeStudent20190992
{
     public static class EmpComparator implements Comparator<Emp> {
            public int compare(Emp x, Emp y) {
                if (x.rating > y.rating) return 1;
                if (x.rating < y.rating) return -1;
                return 0;
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
    public static class YouTubeMapper extends Mapper<Object, Text, Text, DoubleWritable> {
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
            context.write(one_key, one_value);
        }
    }
    public static class YouTubeReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private PriorityQueue<Emp> queue ;
        private Comparator<Emp> comp = new EmpComparator();
        private int topK;
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
            insertEmp(queue, key.toString(), agg_val, topK);
        
        }
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            topK = conf.getInt("topK", -1);
            queue = new PriorityQueue<Emp>( topK , comp);
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            while( queue.size() != 0 ) {
                Emp emp = (Emp) queue.remove();
                context.write( new Text( emp.getCategory() ), new DoubleWritable(emp.getRating()));
            }
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        
        if (otherArgs.length != 3) {
            System.err.println("Usage: TopK <in> <out> <k>");   
            System.exit(2);
        }
        conf.setInt("topK", Integer.parseInt(otherArgs[2]));
        
        Job job = new Job(conf, "YouTube");
        job.setJarByClass(YouTubeStudent20190992.class);
        job.setMapperClass(YouTubeMapper.class);
        job.setReducerClass(YouTubeReducer.class);
        job.setNumReduceTasks(1);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        FileSystem.get(job.getConfiguration()).delete(new Path(otherArgs[1]), true);
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
