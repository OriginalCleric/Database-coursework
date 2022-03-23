import java.io.IOException;
import java.util.StringTokenizer;
import java.util.PriorityQueue;
import java.util.Map;
import java.util.Comparator;
import java.util.AbstractMap;
import java.lang.Integer;
import java.lang.Float;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MapReduce {
  
    public static class TokenizerMapper extends Mapper<Object, Text, Text, DoubleWritable>{
        private Text word = new Text();

        /**
         * Maps input key/value pairs to a set of intermediate key/value pair
         * Called once for each key/value pair in the input split.
         * @param key key in key/value input pair
         * @param value value in key/value input pair
         * @param context allows the Mapper to interact with the rest of the Hadoop system
         */
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String startDate = conf.get("startDate");
            String endDate = conf.get("endDate");
            
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                String s = word.toString();
                String[] array = s.split("\\|");

                // Get results where the date lies between the provided start and end dates
                //if (array[7].length() == 0 || array.length < 22 || array[0].length() == 0 || array[21].length() == 0
                if (array.length < 23 || array[0].length() == 0 || array[21].length() == 0
                || Integer.parseInt(array[0]) < Integer.parseInt(startDate) || Integer.parseInt(array[0]) > Integer.parseInt(endDate)){
                    continue;
                }
                context.write(new Text(array[0]), new DoubleWritable(Float.parseFloat(array[21])));
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        /**
         * Reduces a set of intermediate values which share a key to a smaller set of values, in this case finding the sum
         * of values for each key
         * Called once for each key/value pair in the input split.
         * @param key key in key/value input pair
         * @param value value in key/value input pair
         * @param context allows the Mapper to interact with the rest of the Hadoop system
         */
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

        /**
         * Called once at the beginning of the task to initialise class variables
         * @param context allows the Mapper to interact with the rest of the Hadoop system
         */
        @Override
        public void setup(Context context) throws IOException, InterruptedException{
            this.myQueue = new PriorityQueue<Map.Entry<DoubleWritable,IntWritable>>(Map.Entry.comparingByKey(Comparator.reverseOrder()));
            this.K = Integer.parseInt(context.getConfiguration().get("K"));
        }

        /**
         * Maps input key/value pairs to a set of intermediate key/value pair
         * Called once for each key/value pair in the input split.
         * @param key key in key/value input pair
         * @param value value in key/value input pair
         * @param context allows the Mapper to interact with the rest of the Hadoop system
         */
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] row = value.toString().split("\t");
            DoubleWritable valb = new DoubleWritable(Double.parseDouble(row[1]));
            IntWritable ID = new IntWritable(Integer.parseInt(row[0]));
            
            myQueue.add(new AbstractMap.SimpleEntry<DoubleWritable,IntWritable>(valb,ID));
        }

        /**
         * Called once at the end of the task to write the final values to the context
         * @param context allows the Mapper to interact with the rest of the Hadoop system
         */
        @Override
        public void cleanup(Context context) throws IOException, InterruptedException{
            int max = myQueue.size();
            for (int i=0;i<max;i++)
            {
                Map.Entry<DoubleWritable,IntWritable> current = myQueue.poll();
                context.write( new IntWritable(current.getValue().get()),new DoubleWritable(current.getKey().get()));
            }
        }
    }
    
    public static class SortReducer extends Reducer<IntWritable,DoubleWritable,IntWritable,DoubleWritable> {
        public PriorityQueue<Map.Entry<DoubleWritable,IntWritable>> myQueue;
        private int K=0;

        /**
         * Called once at the beginning of the task to initialise class variables
         * @param context allows the Mapper to interact with the rest of the Hadoop system
         */
        @Override
        public void setup(Context context) throws IOException, InterruptedException{
            this.myQueue = new PriorityQueue<Map.Entry<DoubleWritable,IntWritable>>(Map.Entry.comparingByKey(Comparator.reverseOrder()));
            this.K = Integer.parseInt(context.getConfiguration().get("K"));
        }
        
        /**
         * Reduces a set of intermediate values which share a key to a smaller set of values
         * of values for each key
         * Called once for each key/value pair in the input split.
         * @param key key in key/value input pair
         * @param value value in key/value input pair
         * @param context allows the Mapper to interact with the rest of the Hadoop system
         */
        public void reduce(IntWritable key, Iterable<DoubleWritable> values,Context context) throws IOException, InterruptedException {
           for(DoubleWritable val:values){
                DoubleWritable valb = new DoubleWritable(val.get());
                IntWritable keyb = new IntWritable(key.get());

                myQueue.add(new AbstractMap.SimpleEntry<DoubleWritable,IntWritable>(valb,keyb));
           }
        }

        /**
         * Called once at the end of the task to write the final values to the context
         * @param context allows the Mapper to interact with the rest of the Hadoop system
         */
        @Override
        public void cleanup(Context context) throws IOException, InterruptedException{
            int max = myQueue.size();
            for (int i=0;i<Math.min(max,K);i++){
                Map.Entry<DoubleWritable,IntWritable> current = myQueue.poll();
                context.write( new IntWritable(current.getValue().get()),new DoubleWritable(current.getKey().get()));
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
            SequenceFileAsTextInputFormat.addInputPath(job2, job1OutputPath);
            TextOutputFormat.setOutputPath(job2, new Path(outputPath));
            job2.waitForCompletion(true);
            //System.exit(job2.waitForCompletion(true) ? 0 : 1);
            
        }
    }
}
