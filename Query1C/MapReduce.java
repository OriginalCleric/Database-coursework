
import java.io.IOException;
import java.util.StringTokenizer;
//import java.io.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
//import org.apache.hadoop.io.FloatWritable;
import java.util.PriorityQueue;
import java.util.Map;
import java.util.Comparator;
import java.util.AbstractMap;
import org.apache.hadoop.mapreduce.Job;
import java.lang.Integer;
import java.lang.Float;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

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

                    if (array[7].length() == 0 || array.length < 22 || array[0].length() == 0 
                    || Integer.parseInt(array[0]) < Integer.parseInt(startDate) || Integer.parseInt(array[0]) > Integer.parseInt(endDate)
                    ){
                        continue;
                    }
                    if (array[21].length() == 0){
                        array[21] = "0";
                    }
                    context.write(new Text(array[0]), new DoubleWritable(Float.parseFloat(array[21])));
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

    public static class SortMapper extends Mapper<Object, Text, IntWritable, DoubleWritable>{
        
        public PriorityQueue<Map.Entry<DoubleWritable,IntWritable>> myQueue;
        private int K;

        @Override
        public void setup(Context context) throws IOException,
                                        InterruptedException
        {
            this.myQueue = new PriorityQueue<Map.Entry<DoubleWritable,IntWritable>>(Map.Entry.comparingByKey(Comparator.reverseOrder()));
            this.K = Integer.parseInt(context.getConfiguration().get("K"));
        }
 

        /*
        * ss_sold_date_sk at column 1
        * ss_quantity at column 11
        * ss_item_sk at column 3
        */
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
          // String id = key.toString();
            String[] row = value.toString().split("\t");
            DoubleWritable valb = new DoubleWritable(Double.parseDouble(row[1]));

            
            // we remove the first key-value
            // if it's size increases 10
            /*
            if (myQueue.size() >= 100)
            {
                if (myQueue.peek().getKey().compareTo(valb)>0)
                {
                    return;
                }
                myQueue.poll();
            }
            */
            IntWritable ID = new IntWritable(Integer.parseInt(row[0]));
            
            myQueue.add(new AbstractMap.SimpleEntry<DoubleWritable,IntWritable>(valb,ID));
            
        }

        @Override
        public void cleanup(Context context) throws IOException,
                                           InterruptedException
        {
            int max = myQueue.size();
            for (int i=0;i<max;i++)
            {
                Map.Entry<DoubleWritable,IntWritable> current = myQueue.poll();
                //IntWritable newKey1 = new IntWritable(current.getValue());
                //DoubleWritable newValue1 = new DoubleWritable(current.getKey());
                context.write( new IntWritable(current.getValue().get()),new DoubleWritable(current.getKey().get()));
                //context.write(newKey1,newValue1);
                //context.write(current.getKey(), current.getValue());
               // context.write(current.getValue(), new IntWritable(current.getKey()));
            }
        }
    }
    
    public static class SortReducer extends Reducer<IntWritable,DoubleWritable,IntWritable,DoubleWritable> {

        public PriorityQueue<Map.Entry<DoubleWritable,IntWritable>> myQueue;
        private int K=0;

        @Override
        public void setup(Context context) throws IOException,
                                        InterruptedException
        {
            this.myQueue = new PriorityQueue<Map.Entry<DoubleWritable,IntWritable>>(Map.Entry.comparingByKey(Comparator.reverseOrder()));
            this.K = Integer.parseInt(context.getConfiguration().get("K"));
        }
 

        public void reduce(IntWritable key, Iterable<DoubleWritable> values,Context context) throws IOException, InterruptedException {
            

            
            
           //DoubleWritable valb = new DoubleWritable(0);  
           
           for(DoubleWritable val:values)
           {
                   
                DoubleWritable valb = new DoubleWritable(val.get());
                IntWritable keyb = new IntWritable(key.get());
                Double valc= new Double (val.get());
                /*
                if (myQueue.size() >= 100)
                {
                    //if (myQueue.peek().getKey().compareTo(valc)>0)
                    Double keyc = new Double (myQueue.peek().getKey().get());
                    if (keyc.compareTo(valc)>0)
                    {
                        return;
                    }
                    myQueue.poll();
                }
                */
                myQueue.add(new AbstractMap.SimpleEntry<DoubleWritable,IntWritable>(valb,keyb));
           }

            
        }

        @Override
        public void cleanup(Context context) throws IOException,
                                           InterruptedException
        {
            int max = myQueue.size();
            for (int i=0;i<Math.min(max,K);i++)
            {
                Map.Entry<DoubleWritable,IntWritable> current = myQueue.poll();
                //IntWritable newKey = new IntWritable(current.getValue());
                //DoubleWritable newValue = new DoubleWritable(current.getKey());
                //context.write(current.getValue(), current.getKey());
                context.write( new IntWritable(current.getValue().get()),new DoubleWritable(current.getKey().get()));
                //context.write(newKey,newValue);
                //context.write(current.getKey(),current.getValue());
            }
            
        }
    }

    public static void main(String[] args) throws Exception {
        /*
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
        FileOutputFormat.setOutputPath(job, new Path(args[4]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        */
        if (args.length<5) return;
        String k = args[0];
        String startDate = args[1];
        String endDate = args[2];
        String inputPath = args[3];
        String outputPath = args[4];

        int reducerNum = 1;
        //Job 1: selecting and aggregation
        Configuration conf1 = new Configuration();
        conf1.set("startDate", startDate);
        conf1.set("endDate", endDate);
        conf1.set("K", k);


        Job job1 = Job.getInstance(conf1, "Select");
        job1.setJarByClass(MapReduce.class);
        job1.setMapperClass(TokenizerMapper.class);
        job1.setCombinerClass(IntSumReducer.class);
        job1.setReducerClass(IntSumReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(DoubleWritable.class);
        TextInputFormat.addInputPath(job1, new Path(inputPath));
        Path job1OutputPath = new Path(outputPath+"_temp");
        SequenceFileOutputFormat.setOutputPath(job1, job1OutputPath);
        job1.waitForCompletion(true);
        System.out.println(job1.getNumReduceTasks());
        if (reducerNum>=1)
        {
            Configuration conf2 = new Configuration();
            conf2.set("K", k);
            Job job2 = Job.getInstance(conf2, "Sort");
            job2.setJarByClass(MapReduce.class);
            job2.setMapperClass(SortMapper.class);
            job2.setNumReduceTasks(1);
            job2.setReducerClass(SortReducer.class);
            job2.setOutputKeyClass(IntWritable.class);
            job2.setOutputValueClass(DoubleWritable.class);
            //job2.setOutputKeyClass(Float.class);
            //job2.setOutputValueClass(Integer.class);
            SequenceFileAsTextInputFormat.addInputPath(job2, job1OutputPath);
            TextOutputFormat.setOutputPath(job2, new Path(outputPath));
            job2.waitForCompletion(true);
            //System.exit(job2.waitForCompletion(true) ? 0 : 1);
            
        }
    }
}
