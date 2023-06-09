import java.io.*;
import java.util.*;
import java.time.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.FSDataInputStream;

public class UBERStudent20190992
{
    public static class UBERMapper extends Mapper<Object, Text, Text, Text>
    {
        private Text info = new Text();
        private Text vt = new Text();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), ",");

            String number = itr.nextToken().trim();
            String date = itr.nextToken().trim();
            int year = Integer.parseInt(date.split("/")[2]);
            int month = Integer.parseInt(date.split("/")[0]);
            int day = Integer.parseInt(date.split("/")[1]);
            LocalDate d = LocalDate.of(year, month, day);
            DayOfWeek dayOfWeek = d.getDayOfWeek();
            String dayEng="";
            
            if(dayOfWeek.getValue() == 1) {
                dayEng = "MON";
            }else if (dayOfWeek.getValue() == 2) {
                dayEng = "TUE";
            }else if (dayOfWeek.getValue() == 3) {
                dayEng = "WED";
            }else if (dayOfWeek.getValue() == 4) {
                dayEng = "THR";
            }else if (dayOfWeek.getValue() == 5) {
                dayEng = "FRI";
            }else if (dayOfWeek.getValue() == 6) {
                dayEng = "SAT";
            }else if (dayOfWeek.getValue() == 7) {
                dayEng = "SUN";
            }
           
            String numDay = number + "," + dayEng;
            info.set(numDay);

            String vehicles = itr.nextToken().trim();
            String trips = itr.nextToken().trim();
            String vehtrip = vehicles + "," + trips;
            vt.set(vehtrip);

            context.write(info, vt);


        }
    }

    public static class UBERReducer extends Reducer<Text,Text,Text,Text>{
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            int vSum = 0;
            int tSum = 0;
            Text result = new Text();
            
            for (Text val : values) {
                String vt = val.toString();
                int veh = Integer.parseInt(vt.split(",")[0]);
                int tr = Integer.parseInt(vt.split(",")[1]);
                vSum += veh;
                tSum += tr;
            }
            String total = tSum + "," + vSum;
            result.set(total);
            context.write(key, result);
        }
    }





    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "UBERStudent20190992");

        job.setJarByClass(UBERStudent20190992.class);
        job.setMapperClass(UBERMapper.class);
        job.setReducerClass(UBERReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        FileSystem.get(job.getConfiguration()).delete( new Path(args[1]), true);
        job.waitForCompletion(true);


    }
}
