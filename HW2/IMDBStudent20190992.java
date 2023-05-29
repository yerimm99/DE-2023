import java.util.*;
import java.io.*;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

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

class Movie{
        public String title;
        public double rating;

        public Movie(String _title, double _rating) {
            this.title = _title;
            this.rating = _rating;
        }
        public String getTitle() {
          return title;
        }

        public double getRating() {
          return rating;
        }	
        public String getString() {
            return title + " " + rating;
        }

    }
class DoubleString implements WritableComparable {
    String joinKey = new String();
    String tableName = new String();

    public DoubleString() {}
    public DoubleString( String _joinKey, String _tableName ) {
      joinKey = _joinKey;
      tableName = _tableName;
    }
    public void readFields(DataInput in) throws IOException {
      joinKey = in.readUTF();
      tableName = in.readUTF();
    }
    public void write(DataOutput out) throws IOException {
      out.writeUTF(joinKey);
      out.writeUTF(tableName);
    }
  
    public int compareTo(Object o1) {
        DoubleString o = (DoubleString) o1;
        int ret = joinKey.compareTo( o.joinKey );
        if (ret!=0) return ret;
        return tableName.compareTo( o.tableName );
    }
    public String toString() { 
      return joinKey + " " + tableName; 
    }

}
	
public class IMDBStudent20190992
{
     public static class MovieComparator implements Comparator<Movie> {
            public int compare(Movie x, Movie y) {
                if (x.rating > y.rating) return 1;
                if (x.rating < y.rating) return -1;
                return 0;
            }
        }
    
      public static void insertMovie(PriorityQueue q, String title, double rating, int topK) {
          Movie movie_head = (Movie) q.peek();
          if ( q.size() < topK || movie_head.rating < rating )
          {
              Movie movie = new Movie(title, rating);
              q.add( movie );
              if( q.size() > topK ) q.remove();
          }
      }

     public static class CompositeKeyComparator extends WritableComparator {
        protected CompositeKeyComparator() {
          super(DoubleString.class, true);
        }
        public int compare(WritableComparable w1, WritableComparable w2) {
          DoubleString k1 = (DoubleString)w1;
          DoubleString k2 = (DoubleString)w2;
          int result = k1.joinKey.compareTo(k2.joinKey);
          if(0 == result) {
            result = k1.tableName.compareTo(k2.tableName);
          }
          return result;
          }
     }

     public static class FirstPartitioner extends Partitioner<DoubleString, Text> {
        public int getPartition(DoubleString key, Text value, int numPartition) {
          return key.joinKey.hashCode()%numPartition;
        }
     }
  
     public static class FirstGroupingComparator extends WritableComparator {
        protected FirstGroupingComparator() {
          super(DoubleString.class, true);
        }
        public int compare(WritableComparable w1, WritableComparable w2) {
          DoubleString k1 = (DoubleString)w1;
          DoubleString k2 = (DoubleString)w2;
          return k1.joinKey.compareTo(k2.joinKey);
        }
     }
    public static class IMDBMapper extends Mapper<Object, Text, DoubleString, Text> {
        boolean movieFile = true;
      	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      	    String[] mv = value.toString().split("::");
            DoubleString one_key = new DoubleString();
	    Text one_value = new Text();
            if(movieFile){
                String movie_id = mv[0];
                String title = mv[1];
                String genre = mv[2];

                boolean isFantasy = false;
                StringTokenizer itr = new StringTokenizer(genre, "|");
                while(itr.hasMoreTokens()){
                    if (itr.nextToken().equals("Fantasy")){
                      isFantasy = true;
                      break;
                    }
                 }
                if(isFantasy){
                  one_key = new DoubleString(movie_id, "M");
                  one_value.set("M," + title);
                  context.write(one_key, one_value);
                }
             }
              else{
                String movie_id = mv[1];
                String rating = mv[2];
                one_key = new DoubleString(movie_id, "R");
                one_value.set("R," + rating);
                context.write(one_key, one_value);
              }  
          }
          protected void setup(Context context) throws IOException, InterruptedException {
              String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
              if ( filename.indexOf( "movies.dat" ) != -1 ) movieFile = true;
              else movieFile = false;
        }
    }
    public static class IMDBReducer extends Reducer<DoubleString,Text,Text,DoubleWritable> {
        private PriorityQueue<Movie> queue ;
        private Comparator<Movie> comp = new MovieComparator();
        private int topK;
        public void reduce(DoubleString key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String title = "";
            double sum = 0;
            int i = 0;
            for (Text val : values) {
                String [] data = val.toString().split(",");
                if(i == 0) {
                    if(!data[0].equals("M")) break;
                        title = data[1];
                 } else {
                     sum += Double.parseDouble(data[1]);
                 }
                 i++;
             }
            
            if (sum != 0) {
                double avg = sum / (i - 1);
                insertMovie(queue, title, avg, topK);
              }

        }
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            topK = conf.getInt("topK", -1);
            queue = new PriorityQueue<Movie>( topK , comp);
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            while( queue.size() != 0 ) {
                Movie movie = (Movie) queue.remove();
                context.write( new Text( movie.getTitle() ), new DoubleWritable(movie.getRating()));
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
        conf.setInt("topK", Integer.valueOf(otherArgs[2]));
        
        Job job = new Job(conf, "IMDB");
        job.setJarByClass(IMDBStudent20190992.class);
        job.setMapperClass(IMDBMapper.class);
        job.setReducerClass(IMDBReducer.class);
        job.setNumReduceTasks(1);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
      
        job.setMapOutputKeyClass(DoubleString.class);
        job.setMapOutputValueClass(Text.class);
      
        job.setPartitionerClass(FirstPartitioner.class);
        job.setGroupingComparatorClass(FirstGroupingComparator.class);
        job.setSortComparatorClass(CompositeKeyComparator.class);
		
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        FileSystem.get(job.getConfiguration()).delete(new Path(otherArgs[1]), true);
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
