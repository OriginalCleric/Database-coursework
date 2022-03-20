package Query1B;
import java.io.IOException;
//import java.io.*;

import org.apache.hadoop.conf.Configuration;
import java.util.PriorityQueue;
import java.util.AbstractMap;
//import org.apache.hadoop.io.DoubleWritable;
//import org.apache.hadoop.io.FloatWritable;
import java.util.Map;
import java.util.Comparator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Sorting {
    
    public static class MyMapper extends Mapper<Text, IntWritable, Text, IntWritable>{
        
        public PriorityQueue<Map.Entry<Integer,Text>> myQueue;
        private int K=0;

        @Override
        public void setup(Context context) throws IOException,
                                        InterruptedException
        {
            this.myQueue = new PriorityQueue<Map.Entry<Integer,Text>>(Map.Entry.comparingByValue(Comparator.reverseOrder()));
            this.K = Integer.parseInt(context.getConfiguration().get("K"));
        }
 

        /*
        * ss_sold_date_sk at column 1
        * ss_quantity at column 11
        * ss_item_sk at column 3
        */
        public void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
          // String id = key.toString();
           int numSold = value.get();

            // we remove the first key-value
            // if it's size increases 10
            if (myQueue.size() >= K)
            {
                if (myQueue.peek().getKey().compareTo(numSold)>0)
                {
                    return;
                }
                myQueue.poll();
            }
            myQueue.add(new AbstractMap.SimpleEntry<Integer,Text>(10,key));
        }

        @Override
        public void cleanup(Context context) throws IOException,
                                           InterruptedException
        {
            Object[] result = myQueue.toArray();
            for (int i=0;i<K;i++)
            {
                Map.Entry<Integer,Text> current = (Map.Entry<Integer,Text>) result[i];
                context.write(current.getValue(), new IntWritable(current.getKey()));
            }
        }
    }

    public static class MyReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

        public PriorityQueue<Map.Entry<Integer,Text>> myQueue;
        private int K=0;

        @Override
        public void setup(Context context) throws IOException,
                                        InterruptedException
        {
            this.myQueue = new PriorityQueue<Map.Entry<Integer,Text>>(Map.Entry.comparingByValue(Comparator.reverseOrder()));
            this.K = Integer.parseInt(context.getConfiguration().get("K"));
        }
 

        public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
           int numSold = 0;
           
           for(IntWritable val:values)
           {
                numSold = val.get();
           }

            // we remove the first key-value
            // if it's size increases 10
            if (myQueue.size() >= K)
            {
                if (myQueue.peek().getKey().compareTo(numSold)>0)
                {
                    return;
                }
                myQueue.poll();
            }
            myQueue.add(new AbstractMap.SimpleEntry<Integer,Text>(10,key));
        }

        @Override
        public void cleanup(Context context) throws IOException,
                                           InterruptedException
        {
            Object[] result = myQueue.toArray();
            for (int i=0;i<K;i++)
            {
                Map.Entry<Integer,Text> current = (Map.Entry<Integer,Text>) result[i];
                context.write(current.getValue(), new IntWritable(current.getKey()));
            }
        }
    }

}
