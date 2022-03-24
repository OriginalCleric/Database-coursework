import java.io.IOException;
import java.util.StringTokenizer;
import java.util.PriorityQueue;
import java.util.AbstractMap;
import java.util.Map;
import java.util.Comparator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Solution {
	public static class CompositeKey<K1 extends Comparable<K1>,K2 extends Comparable<K2>> implements Comparable<CompositeKey<K1,K2>> {
		public K1 key1; public K2 key2;

		public CompositeKey(K1 k1,K2 k2) {
			key1=k1; key2=k2;
		}

		// Sort by key1 then by key2
		public int compareTo(CompositeKey<K1,K2> other) {
			int result = key1.compareTo(other.key1);
			if(result==0) { result = key2.compareTo(other.key2);}
			return result;
		}
	}
   
	public static class ReduceJoin extends Reducer<IntWritable,Text,IntWritable,Text> {
		public PriorityQueue<Map.Entry<CompositeKey<Integer,Double>,Integer>> myQueue;
		private int K;

		/**
         * Called once at the beginning of the task to initialise class variables
         * @param context allows the Mapper to interact with the rest of the Hadoop system
         */
		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			this.myQueue = new PriorityQueue<Map.Entry<CompositeKey<Integer,Double>,Integer>>(Map.Entry.comparingByKey(Comparator.reverseOrder()));
			this.K = Integer.parseInt(context.getConfiguration().get("K"));
		}

		// Aggregate the net paid
		/**
         * Reduces a set of intermediate values which share a key to a smaller set of values, in this case
		 * aggregating the net paid for each key
         * @param key key in key/value input pair
         * @param values all values matching the key
         * @param context allows the Mapper to interact with the rest of the Hadoop system
         */
		public void reduce(IntWritable key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
			if (key==null) return;

			Double sum = 0.0;
			String floor="0";

			for (Text val : values){
				String[] parts = val.toString().split(",");
				
				if (parts[0].equals( "FLOOR")){
					floor = parts[1];
				}
				else if (parts[0].equals("NET")){
					sum += Double.parseDouble(parts[1]);
				}
			}

			if (sum == 0) return;
		
			CompositeKey<Integer,Double> myKey = new Solution.CompositeKey<Integer,Double>(Integer.parseInt(floor),sum);

			// Limit queue size to input parameter K
			if (myQueue.size() >= K){
				if (myQueue.peek().getKey().compareTo(myKey)>0){ return;}
				myQueue.poll();
			}
			myQueue.add(new AbstractMap.SimpleEntry<CompositeKey<Integer,Double> ,Integer>(myKey,key.get()));
		}

		/**
         * Called once at the end of the task to write the final values to the context
         * @param context allows the Mapper to interact with the rest of the Hadoop system
         */
		@Override
		public void cleanup(Context context) throws IOException,InterruptedException{
			int max = myQueue.size();

			for (int i=0;i<max;i++){
				Map.Entry<CompositeKey<Integer,Double> ,Integer> current = myQueue.poll();
				Text result = new Text(current.getKey().key1.toString()+"\t"+current.getKey().key2.toString());
				context.write(new IntWritable(current.getValue()), result);
			}
		}
  	}

	public static class StoreMapper extends Mapper<Object, Text, IntWritable, Text>{
		private Text word = new Text();
		public static String path = "";

		/**
         * Maps input key/value pairs to a set of intermediate key/value pair, sorted by net_profit
         * Called once for each key/value pair in the input split.
		 * if the entry is on the correct floor: output key: store_sk, value: floor
         * @param key key in key/value input pair
         * @param value value in key/value input pair
         * @param context allows the Mapper to interact with the rest of the Hadoop system
         */
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String row = value.toString();
			String[] columns = row.split("\\|");
			if(columns[7].length() == 0 || columns[0].length() == 0){
				return;
			}
			context.write(new IntWritable(Integer.parseInt(columns[0])), new Text("FLOOR,"+columns[7]));
		}
	}
	
	
	public static class SaleMapper extends Mapper<Object, Text, IntWritable,Text>{
		private Text word = new Text();

		/**
         * Maps input key/value pairs to a set of intermediate key/value pair, sorted by net_profit
         * Called once for each key/value pair in the input split.
         * @param key key in key/value input pair
         * @param value value in key/value input pair
         * @param context allows the Mapper to interact with the rest of the Hadoop system
         */
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			Configuration conf = context.getConfiguration();
			String startDate = conf.get("startDate");
			String endDate = conf.get("endDate");

			String s = value.toString();
			String[] array = s.split("\\|");

			// Get results where the date lies between the provided start and end dates
			if (array.length < 21|| array[7].length() == 0 || array[0].length() == 0 || array[20].length() == 0
			|| Integer.parseInt(array[0]) < Integer.parseInt(startDate) || Integer.parseInt(array[0]) > Integer.parseInt(endDate)){
				return;
			}
			context.write(new IntWritable(Integer.parseInt(array[7])), new Text("NET,"+array[20]));	
		}
	}

	/**
     * Driver Function to initialise the mapreduce job(s)
     * @param args arguments passed in to the function through the command line. Format k, startDate, endDate, file1_inputPath, file2_inputPath, outputPath
     * @throws Exception
     */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		//reading the arguments
		String k = args[0];
		String startDate = args[1];
		String endDate = args[2];
		String salesFile = args[3];
		String storeFile =args[4];
		String outputDir = args[5];

		//passing arguments to hadoop
		conf.set("startDate", startDate);
		conf.set("endDate", endDate);
		conf.set("K", k);
		
		Job job = Job.getInstance(conf, "Solution");
		job.setJarByClass(Solution.class);
		job.setReducerClass(ReduceJoin.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		MultipleInputs.addInputPath(job, new Path(salesFile),TextInputFormat.class,SaleMapper.class);
		MultipleInputs.addInputPath(job, new Path(storeFile),TextInputFormat.class,StoreMapper.class);
		FileOutputFormat.setOutputPath(job,new Path(outputDir));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}