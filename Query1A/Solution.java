import java.io.IOException;
import java.util.PriorityQueue;
import java.util.AbstractMap;
import java.util.Map;
import java.util.Comparator;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsBinaryInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;


public class Solution {
    public static class MyMapper extends Mapper<Object, Text, IntWritable, DoubleWritable>{
        int startDate;
        int endDate;
        
        /**
         * Called once at the beginning of the task pass in parameters through the context
         * @param context allows the Mapper to interact with the rest of the Hadoop system
         */
        public void setup(Context context) throws IOException, InterruptedException{
            Configuration conf = context.getConfiguration();
            startDate = Integer.parseInt(conf.get("startDate"));
            endDate = Integer.parseInt(conf.get("endDate"));
        }

        /**
         * Maps input key/value pairs to a set of intermediate key/value pair
         * Called once for each key/value pair in the input split.
         * @param key key in key/value input pair
         * @param value value in key/value input pair
         * @param context allows the Mapper to interact with the rest of the Hadoop system
         */
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] row = value.toString().split("\\|");
            Double num = 0.0;

            // Get results where the date lies between the provided start and end dates
            if ( row.length < 21 || row[0].length()==0 || row[7].length() == 0 || row[20].length() == 0
            || Integer.parseInt(row[0]) < startDate || Integer.parseInt(row[0]) > endDate){
                return;
            }
            context.write(new IntWritable(Integer.parseInt(row[7])), new DoubleWritable(Double.parseDouble(row[20])));
        }
    }

    public static class MyReducer extends Reducer<IntWritable,DoubleWritable,IntWritable,DoubleWritable> {
        public PriorityQueue<Map.Entry<Double,Integer>> myQueue;
        private int K;

        /**
         * Called once at the beginning of the task to initialise class variables
         * @param context allows the Mapper to interact with the rest of the Hadoop system
         */
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            this.myQueue = new PriorityQueue<Map.Entry<Double,Integer>>(Map.Entry.comparingByKey());
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
        public void reduce(IntWritable key, Iterable<DoubleWritable> values,Context context) throws IOException, InterruptedException {
            Double sum = 0.0;
            for (DoubleWritable val : values) {
                sum += val.get();
            }
            
            // Limit queue size to input parameter K
            if (myQueue.size() >= K){
                if (myQueue.peek().getKey().compareTo(sum)>0){ return;}
                myQueue.poll();
            }
            myQueue.add(new AbstractMap.SimpleEntry<Double,Integer>(sum,key.get()));
        }

        /**
         * Called once at the end of the task to write the final values to the context
         * @param context allows the Mapper to interact with the rest of the Hadoop system
         */
        @Override
        public void cleanup(Context context) throws IOException, InterruptedException{
            int max = myQueue.size();
            for (int i=0;i<max;i++) {
                Map.Entry<Double,Integer> current = myQueue.poll();
                context.write(new IntWritable(current.getValue()), new DoubleWritable(current.getKey()));
            }
        }
    }


    public static class SortMapper extends Mapper<Object, Text, IntWritable, DoubleWritable>{
        public PriorityQueue<Map.Entry<Double,Integer>> myQueue;
        private int K;

        /**
         * Called once at the beginning of the task to initialise class variables
         * @param context allows the Mapper to interact with the rest of the Hadoop system
         */
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            this.myQueue = new PriorityQueue<Map.Entry<Double,Integer>>(Map.Entry.comparingByKey(Comparator.reverseOrder()));
            this.K = Integer.parseInt(context.getConfiguration().get("K"));
        }

        /**
         * Maps input key/value pairs to a set of intermediate key/value pair, sorted by net_profit
         * Called once for each key/value pair in the input split.
         * @param key key in key/value input pair
         * @param value value in key/value input pair
         * @param context allows the Mapper to interact with the rest of the Hadoop system
         */
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] row = value.toString().split("\t");
            Double numPaid = Double.parseDouble(row[1]);
            int ID = Integer.parseInt(row[0]);
            
            if (myQueue.size() >= K){
                if (myQueue.peek().getKey().compareTo(numPaid)>0){return;}
                myQueue.poll();
            }
            
            myQueue.add(new AbstractMap.SimpleEntry<Double,Integer>(numPaid,ID));
        }

        /**
         * Called once at the end of the task to write the final values to the context
         * @param context allows the Mapper to interact with the rest of the Hadoop system
         */
        @Override
        public void cleanup(Context context) throws IOException, InterruptedException{
            int max = myQueue.size();
            for (int i=0;i<max;i++){
                Map.Entry<Double,Integer> current = myQueue.poll();
                context.write( new IntWritable(current.getValue()),new DoubleWritable(current.getKey()));
            }
        }
    }

    public static class SortReducer extends Reducer<IntWritable,DoubleWritable,IntWritable,DoubleWritable> {
        public PriorityQueue<Map.Entry<Double,Integer>> myQueue;
        private int K=0;

        /**
         * Called once at the beginning of the task to initialise class variables
         * @param context allows the Mapper to interact with the rest of the Hadoop system
         */
        @Override
        public void setup(Context context) throws IOException, InterruptedException{
            this.myQueue = new PriorityQueue<Map.Entry<Double,Integer>>(Map.Entry.comparingByKey(Comparator.reverseOrder()));
            this.K = Integer.parseInt(context.getConfiguration().get("K"));
        }
 
        /**
         * Reduces a set of intermediate values which share a key to a smaller set of values
         * Called once for each key/value pair in the input split.
         * @param key key in key/value input pair
         * @param value value in key/value input pair
         * @param context allows the Mapper to interact with the rest of the Hadoop system
         */
        public void reduce(IntWritable key, Iterable<DoubleWritable> values,Context context) throws IOException, InterruptedException {
            Double numPaid = 0.0;
           
           for(DoubleWritable val:values){
                numPaid = val.get();
                if (myQueue.size() >= K){
                    if (myQueue.peek().getKey().compareTo(numPaid)>0){return;}
                    myQueue.poll();
                }
                myQueue.add(new AbstractMap.SimpleEntry<Double,Integer>(numPaid,key.get()));
           }   
        }

        /**
         * Called once at the end of the task to write the final values to the context
         * @param context allows the Mapper to interact with the rest of the Hadoop system
         */
        @Override
        public void cleanup(Context context) throws IOException, InterruptedException{
            int max = myQueue.size();
            for (int i=0;i<max;i++){
                Map.Entry<Double,Integer> current = myQueue.poll();
                context.write(new IntWritable(current.getValue()), new DoubleWritable(current.getKey()));
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

        int reducerNum = 8;
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
        job1.setOutputValueClass(DoubleWritable.class);
        TextInputFormat.addInputPath(job1, new Path(inputPath));

        if (reducerNum>1)
        {
            Path job1OutputPath = new Path(outputPath+"_temp");
            SequenceFileOutputFormat.setOutputPath(job1, job1OutputPath);
            job1.waitForCompletion(true);

            Configuration conf2 = new Configuration();
            conf2.set("K", k);
            Job job2 = Job.getInstance(conf2, "Sort");
            job2.setJarByClass(Solution.class);
            job2.setMapperClass(SortMapper.class);
            job2.setNumReduceTasks(1);
            job2.setReducerClass(SortReducer.class);
            job2.setOutputKeyClass(IntWritable.class);
            job2.setOutputValueClass(DoubleWritable.class);
            SequenceFileAsTextInputFormat.addInputPath(job2, job1OutputPath);
            TextOutputFormat.setOutputPath(job2, new Path(outputPath));
            job2.waitForCompletion(true);
            //System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }else
        {
            Path job1OutputPath = new Path(outputPath);
            TextFileOutputFormat.setOutputPath(job1, job1OutputPath);
            job1.waitForCompletion(true);
        }
    }
}