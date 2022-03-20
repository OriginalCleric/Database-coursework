import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsBinaryInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import java.util.PriorityQueue;
import java.util.AbstractMap;
//import org.apache.hadoop.io.DoubleWritable;
//import org.apache.hadoop.io.FloatWritable;
import java.util.Map;
import java.util.Comparator;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
public class Solution {
     
    public static class MyMapper extends Mapper<Object, Text, IntWritable, FloatWritable>{
        
        int startDate;
        int endDate;
        public void setup(Context context) throws IOException, InterruptedException{
            Configuration conf = context.getConfiguration();
            startDate = Integer.parseInt(conf.get("startDate"));
            endDate = Integer.parseInt(conf.get("endDate"));
        }

        /*
        * ss_sold_date_sk at column 1
        * ss_net_paid at column 21
        * ss_store_sk at column 8
        */
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] row = value.toString().split("\\|");
            if ( row.length < 21 || row[0].length()==0 || row[7].length() == 0
            || Integer.parseInt(row[0]) < startDate || Integer.parseInt(row[0]) > endDate){
                return;
            }

            int ID = Integer.parseInt(row[7]);
            float num = 0;
            if (row[20].length() != 0){
                num = Float.parseFloat(row[20]);
            }
            context.write(new IntWritable(ID), new FloatWritable(num));
        }
    }

    public static class MyReducer extends Reducer<IntWritable,FloatWritable,IntWritable,FloatWritable> {

        public PriorityQueue<Map.Entry<Float,Integer>> myQueue;
        private int K;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            this.myQueue = new PriorityQueue<Map.Entry<Float,Integer>>(Map.Entry.comparingByKey());
            this.K = Integer.parseInt(context.getConfiguration().get("K"));
        }

        public void reduce(IntWritable key, Iterable<FloatWritable> values,Context context) throws IOException, InterruptedException {
            float sum = 0;
            for (FloatWritable val : values) {
                sum += val.get();
            }

            if (myQueue.size() >= K){
                if (myQueue.peek().getKey().compareTo(sum)>0){
                    return;
                }
                myQueue.poll();
            }
            myQueue.add(new AbstractMap.SimpleEntry<Float,Integer>(sum,key.get()));
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException{
            int max = myQueue.size();
            for (int i=0;i<max;i++) {
                Map.Entry<Float,Integer> current = myQueue.poll();
                context.write(new IntWritable(current.getValue()), new FloatWritable(current.getKey()));
            }
        }
    }


    public static class SortMapper extends Mapper<Object, Text, IntWritable, FloatWritable>{
        
        public PriorityQueue<Map.Entry<Float,Integer>> myQueue;
        private int K;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            this.myQueue = new PriorityQueue<Map.Entry<Float,Integer>>(Map.Entry.comparingByKey(Comparator.reverseOrder()));
            this.K = Integer.parseInt(context.getConfiguration().get("K"));
        }

        /*
        * ss_sold_date_sk at column 1
        * ss_net_paid at column 21
        * ss_store_sk at column 8
        */
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] row = value.toString().split("\t");
            float numPaid = Float.parseFloat(row[1]);
            
            // we remove the first key-value
            if (myQueue.size() >= K)
            {
                if (myQueue.peek().getKey().compareTo(numPaid)>0){return;}
                myQueue.poll();
            }
            int ID = Integer.parseInt(row[0]);
            myQueue.add(new AbstractMap.SimpleEntry<Float,Integer>(numPaid,ID));
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException{
            int max = myQueue.size();
            for (int i=0;i<max;i++){
                Map.Entry<Float,Integer> current = myQueue.poll();
                context.write( new IntWritable(current.getValue()),new FloatWritable(current.getKey()));
            }
        }
    }

    public static class SortReducer extends Reducer<IntWritable,FloatWritable,IntWritable,FloatWritable> {

        public PriorityQueue<Map.Entry<Float,Integer>> myQueue;
        private int K=0;

        @Override
        public void setup(Context context) throws IOException, InterruptedException{
            this.myQueue = new PriorityQueue<Map.Entry<Float,Integer>>(Map.Entry.comparingByKey(Comparator.reverseOrder()));
            this.K = Integer.parseInt(context.getConfiguration().get("K"));
        }
 

        public void reduce(IntWritable key, Iterable<FloatWritable> values,Context context) throws IOException, InterruptedException {
            float numPaid = 0;
           
           for(FloatWritable val:values){
                numPaid = val.get();
                if (myQueue.size() >= K){
                    if (myQueue.peek().getKey().compareTo(numPaid)>0){return;}
                    myQueue.poll();
                }
                myQueue.add(new AbstractMap.SimpleEntry<Float,Integer>(numPaid,key.get()));
           }   
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException{
            int max = myQueue.size();
            for (int i=0;i<max;i++){
                Map.Entry<Float,Integer> current = myQueue.poll();
                context.write(new IntWritable(current.getValue()), new FloatWritable(current.getKey()));
            } 
        }
    }
    /*
    *Driver
    */
    public static void main(String[] args) throws Exception {
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
        job1.setNumReduceTasks(reducerNum);
        job1.setJarByClass(Solution.class);
        job1.setMapperClass(MyMapper.class);
        job1.setReducerClass(MyReducer.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(FloatWritable.class);
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
            job2.setJarByClass(Solution.class);
            job2.setMapperClass(SortMapper.class);
            job2.setNumReduceTasks(1);
            job2.setReducerClass(SortReducer.class);
            job2.setOutputKeyClass(IntWritable.class);
            job2.setOutputValueClass(FloatWritable.class);
            SequenceFileAsTextInputFormat.addInputPath(job2, job1OutputPath);
            TextOutputFormat.setOutputPath(job2, new Path(outputPath));
            job2.waitForCompletion(true);
            //System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }
    }
}