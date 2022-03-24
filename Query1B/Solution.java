import java.io.IOException;
import java.util.PriorityQueue;
import java.util.AbstractMap;
import java.util.Map;
import java.util.Comparator;
import java.util.HashMap;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsBinaryInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

public class Solution {

    public static class CompositeKey<K1 extends Comparable<K1>,K2 extends Comparable<K2>> implements Comparable<CompositeKey<K1,K2>>
    {
        public K1 key1; public K2 key2;

        public CompositeKey(K1 k1,K2 k2){
            key1=k1; key2=k2;
        }

        public int compareTo(CompositeKey<K1,K2> other) {
            int result = key1.compareTo(other.key1);
            if(result==0){ result = key2.compareTo(other.key2);}
            return result;
        }
    }
     
    public static class MyMapper extends Mapper<Object, Text, IntWritable, IntWritable>{
        Map<Integer,Integer> localSum;
        int startDate;
        int endDate;

        /**
         * Called once at the beginning of the task pass in parameters through the context
         * @param context allows the Mapper to interact with the rest of the Hadoop system
         */
        public void setup(Context context) throws IOException,InterruptedException{
            Configuration conf = context.getConfiguration();
            this.startDate = Integer.parseInt(conf.get("startDate"));
            this.endDate = Integer.parseInt(conf.get("endDate"));
            this.localSum = new HashMap<Integer, Integer>();
        }

        /**
         * Maps input key/value pairs to a set of intermediate key/value pair
         * Called once for each key/value pair in the input split.
         * @param key key in key/value input pair
         * @param value value in key/value input pair
         * @param context allows the Mapper to interact with the rest of the Hadoop system
         */
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String[] row = value.toString().split("\\|");

            // Get results where the date lies between the provided start and end dates
            if ( row.length < 11 || row[2].length()==0 || row[0].length()==0
            || Integer.parseInt(row[0]) < startDate || Integer.parseInt(row[0]) > endDate){
                return;
            }

            if (row[10].length() != 0){
                int ID = Integer.parseInt(row[2]);
                int num = Integer.parseInt(row[10]);
                context.write(new IntWritable(ID), new IntWritable(num));
                
                
                if (!this.localSum.containsKey(ID))
                {
                    this.localSum.put(ID, num);
                }else
                {
                    this.localSum.replace(ID, num+localSum.get(ID));
                }
            }
        }

        /**
         * Called once at the end of the task to write the final values to the context
         * @param context allows the Mapper to interact with the rest of the Hadoop system
         */
        @Override
        public void cleanup(Context context) throws IOException, InterruptedException{
            /*
            for (Map.Entry<Integer,Integer> i: localSum.entrySet()){
               // context.write(new IntWritable(i.getKey()), new IntWritable(i.getValue()));
            }
            */
        }
    }

    public static class MyReducer extends Reducer<IntWritable,IntWritable,Text,IntWritable> {
        public PriorityQueue<Map.Entry<CompositeKey<Integer,Integer>,Integer>> myQueue;
        private int K;

        /**
         * Called once at the beginning of the task to initialise class variables
         * @param context allows the Mapper to interact with the rest of the Hadoop system
         */
        @Override
        public void setup(Context context) throws IOException, InterruptedException{
            this.myQueue = new PriorityQueue<Map.Entry<CompositeKey<Integer,Integer>,Integer>>(Map.Entry.comparingByKey());
            this.K = Integer.parseInt(context.getConfiguration().get("K"));
        }

        /**
         * Reduces a set of intermediate values which share a key to a smaller set of values, in this case finding the sum
         * of values for each key
         * Called once for each key/value pair in the input split.
         * @param key key in key/value input pair
         * @param value value in key/value input pair
         * @param context allows the Mapper to interact with the rest of the Hadoop system
         */
        public void reduce(IntWritable key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            int ID = key.get();
            CompositeKey<Integer,Integer> newKey = new CompositeKey<Integer,Integer>(sum,ID);

            // Limit queue size to input parameter K
            if (myQueue.size() >= K){
                if (myQueue.peek().getKey().compareTo(newKey)>0){ return;}
                myQueue.poll();
            }
            myQueue.add(new AbstractMap.SimpleEntry<CompositeKey<Integer,Integer> ,Integer>(newKey,1));
        }

        /**
         * Called once at the end of the task to write the final values to the context
         * @param context allows the Mapper to interact with the rest of the Hadoop system
         */
        @Override
        public void cleanup(Context context) throws IOException, InterruptedException{
            int max = myQueue.size();
            CompositeKey<Integer,Integer>[] results = new CompositeKey[max];

            for (int i=0;i<max;i++){
                results[i] = myQueue.poll().getKey(); 
            }

            for (int i = max-1;i>=0;i--){
                context.write(new Text("ss_item_sk:"+results[i].key2.toString()), new IntWritable(results[i].key1));
            }
        }
    }

    /**
     * Driver Function to initialise the mapreduce job(s)
     * @param args arguments passed in to the function through the command line. Format k, startDate, endDate, inputPath, outputPath
     * @throws Exception
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
        
        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        TextInputFormat.addInputPath(job1, new Path(inputPath));

        TextOutputFormat.setOutputPath(job1, new Path(outputPath));
        job1.waitForCompletion(true);
    
    }
}
