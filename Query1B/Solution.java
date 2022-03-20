
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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
public class Solution {

    public class CompositeKey<K1 extends Comparable<K1>,K2 extends Comparable<K2>> implements Comparable<CompositeKey<K1,K2>>
    {
        public K1 key1;
        public K2 key2;

        public CompositeKey(K1 k1,K2 k2)
        {
            key1=k1;
            key2=k2;
        }

        public int compareTo(CompositeKey<K1,K2> other)
        {
            int result = key1.compareTo(other.key1);
            if(result==0)
            {
                result = key2.compareTo(other.key2);
            }
            return result;
        }

    }
     
    public static class MyMapper extends Mapper<Object, Text, IntWritable, IntWritable>{
        
        int startDate;
        int endDate;
        public void setup(Context context) throws IOException,
        InterruptedException
        {
            Configuration conf = context.getConfiguration();
            startDate = Integer.parseInt(conf.get("startDate"));
            endDate = Integer.parseInt(conf.get("endDate"));
        }

        /*
        * ss_sold_date_sk at column 1
        * ss_quantity at column 11
        * ss_item_sk at column 3
        */
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
           // System.out.println(value.toString());
            String[] row = value.toString().split("\\|");
            //System.out.println(value.toString());
            if ( row.length < 21 || row[0].length()==0|| Integer.parseInt(row[0]) < startDate || Integer.parseInt(row[0]) > endDate
            ){
               // context.write(new Text(value.toString()+"\n"), new IntWritable(1));
                return;
            }

            int ID = Integer.parseInt(row[2]);
            int num = 0;
            if (row[10].length() != 0){
                num = Integer.parseInt(row[10]);
            }
            context.write(new IntWritable(ID), new IntWritable(num));
        
        }
    }

    public static class MyReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {

        public PriorityQueue<Map.Entry<CompositeKey<Integer,Integer>,Integer>> myQueue;
        private int K;

        @Override
        public void setup(Context context) throws IOException,
                                        InterruptedException
        {
           // this.myQueue = new PriorityQueue<Map.Entry<Integer,Text>>(Map.Entry.comparingByKey(Comparator.reverseOrder()));
            this.myQueue = new PriorityQueue<Map.Entry<CompositeKey<Integer,Integer>,Integer>>(Map.Entry.comparingByKey());
            this.K = Integer.parseInt(context.getConfiguration().get("K"));
        }

        public void reduce(IntWritable key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            CompositeKey<Integer,Integer> newKey = new CompositeKey<Integer,Integer>(sum,key.get());
            if (myQueue.size() >= K)
            {
                if (myQueue.peek().getKey().compareTo(newKey)>0)
                {
                    return;
                }
                myQueue.poll();
            }
            myQueue.add(new AbstractMap.SimpleEntry<CompositeKey<Integer,Integer> ,Integer>(newKey,key.get()));
           // context.write(key, new IntWritable(sum));
        }

        @Override
        public void cleanup(Context context) throws IOException,
                                           InterruptedException
        {
            int max = myQueue.size();
            for (int i=0;i<max;i++)
            {
                Map.Entry<CompositeKey<Integer,Integer> ,Integer> current = myQueue.poll();
                context.write(new IntWritable(current.getValue()), new IntWritable(current.getKey().key1));
            }
        }
    }

  
    public static class SortMapper extends Mapper<Object, Text, IntWritable, IntWritable>{
        
        public PriorityQueue<Map.Entry<CompositeKey<Integer,Integer>,Integer>> myQueue;
        private int K;

        @Override
        public void setup(Context context) throws IOException,
                                        InterruptedException
        {
            this.myQueue = new PriorityQueue<Map.Entry<CompositeKey<Integer,Integer> ,Integer>>(Map.Entry.comparingByKey(Comparator.reverseOrder()));
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
            int numSold = Integer.parseInt(row[1]);
            
            // we remove the first key-value#
            // if it's size increases 10
            int ID = Integer.parseInt(row[0]);
            CompositeKey<Integer,Integer> newKey = new CompositeKey<Integer,Integer>(numSold,ID);
            
            if (myQueue.size() >= K)
            {
                if (myQueue.peek().getKey().compareTo(newKey)>0)
                {
                    return;
                }
                myQueue.poll();
            }
           
            
            myQueue.add(new AbstractMap.SimpleEntry<CompositeKey<Integer,Integer> ,Integer>(newKey,ID));
            
        }

        @Override
        public void cleanup(Context context) throws IOException,
                                           InterruptedException
        {
            int max = myQueue.size();
            for (int i=0;i<max;i++)
            {
                Map.Entry<CompositeKey<Integer,Integer> ,Integer> current = myQueue.poll();
                context.write( new IntWritable(current.getValue()),new IntWritable(current.getKey().key1));
               // context.write(current.getValue(), new IntWritable(current.getKey()));
            }
        }
    }

    public static class SortReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {

        public PriorityQueue<Map.Entry<CompositeKey<Integer,Integer> ,Integer>> myQueue;
        private int K=0;

        @Override
        public void setup(Context context) throws IOException,
                                        InterruptedException
        {
            this.myQueue = new PriorityQueue<Map.Entry<CompositeKey<Integer,Integer> ,Integer>>(Map.Entry.comparingByKey(Comparator.reverseOrder()));
            this.K = Integer.parseInt(context.getConfiguration().get("K"));
        }
 

        public void reduce(IntWritable key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
            

            
            int numSold = 0;
            CompositeKey<Integer,Integer> newKey = new CompositeKey<Integer,Integer>(numSold,key.get());
           for(IntWritable val:values)
           {
                numSold = val.get();
                if (myQueue.size() >= K)
                {
                    if (myQueue.peek().getKey().compareTo(newKey)>0)
                    {
                        return;
                    }
                    myQueue.poll();
                }
                myQueue.add(new AbstractMap.SimpleEntry<CompositeKey<Integer,Integer> ,Integer>(newKey,key.get()));
           }

            
        }

        @Override
        public void cleanup(Context context) throws IOException,
                                           InterruptedException
        {
            int max = myQueue.size();
            for (int i=0;i<max;i++)
            {
                Map.Entry<CompositeKey<Integer,Integer> ,Integer> current = myQueue.poll();
                context.write(new IntWritable(current.getValue()), new IntWritable(current.getKey().key1));
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
        job1.setOutputValueClass(IntWritable.class);
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
            job2.setOutputValueClass(IntWritable.class);
            SequenceFileAsTextInputFormat.addInputPath(job2, job1OutputPath);
            TextOutputFormat.setOutputPath(job2, new Path(outputPath));
            job2.waitForCompletion(true);
            //System.exit(job2.waitForCompletion(true) ? 0 : 1);
            
        }
    }
}
