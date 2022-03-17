package Query1A;
import java.io.IOException;
import java.util.StringTokenizer;
//import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
//import org.apache.hadoop.io.FloatWritable;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MapReduce {
  
    public static class TokenizerMapper extends Mapper<Object, Text, Text, DoubleWritable>{
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String startDate = conf.get("startDate");
            String endDate = conf.get("endDate");
            
            StringTokenizer itr = new StringTokenizer(value.toString());
                while (itr.hasMoreTokens()) {
                    word.set(itr.nextToken());
                    String s = word.toString();
                    String[] array = s.split("\\|");

                    if (array[7].length() == 0 || array.length < 21 || array[0].length() == 0 
                    || Integer.parseInt(array[0]) < Integer.parseInt(startDate) || Integer.parseInt(array[0]) > Integer.parseInt(endDate)
                    ){
                        continue;
                    }
                    if (array[20].length() == 0){
                        array[20] = "0";
                    }
                    context.write(new Text(array[7]), new DoubleWritable(Float.parseFloat(array[20])));
                }
            }
    }

    public static class IntSumReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values,Context context) throws IOException, InterruptedException {
            float sum = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        String k = args[0];
        String startDate = args[1];
        String endDate = args[2];

        conf.set("startDate", startDate);
        conf.set("endDate", endDate);

        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(MapReduce.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[3]));
        FileOutputFormat.setOutputPath(job, new Path(args[5]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
