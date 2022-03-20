package Query1B;
import java.io.IOException;
import java.util.StringTokenizer;
//import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.DoubleWritable;
//import org.apache.hadoop.io.FloatWritable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MapReduce {
    
    public static class MyMapper extends Mapper<Object, Text, Text, IntWritable>{
        
        /*
        * ss_sold_date_sk at column 1
        * ss_quantity at column 11
        * ss_item_sk at column 3
        */
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String startDate = conf.get("startDate");
            String endDate = conf.get("endDate");
        
            String[] row = value.toString().split("\\|");

            if ( row.length < 21 ||row[0].length() == 0 || row[0].length() == 0 
            || Integer.parseInt(row[0]) < Integer.parseInt(startDate) || Integer.parseInt(row[0]) > Integer.parseInt(endDate)
            ){
                return;
            }


            if (row[10].length() == 0){
                row[10] = "0";
            }
            context.write(new Text(row[2]), new IntWritable(Integer.parseInt(row[10])));
        
        }
    }

    public static class MyReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        String k = args[0];
        String startDate = args[1];
        String endDate = args[2];

        conf.set("startDate", startDate);
        conf.set("endDate", endDate);
        conf.set("K", k);

        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(MapReduce.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[3]));
        FileOutputFormat.setOutputPath(job, new Path(args[5]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
