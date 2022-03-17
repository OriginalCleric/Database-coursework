import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Solution {

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

    //TODO
    
    Job job = Job.getInstance(conf, "Solution");
    job.setJarByClass(Solution.class);
    job.setReducerClass(ReduceJoin.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    MultipleInputs.addInputPath(job, new Path(salesFile),TextInputFormat.class,SaleMapper.class);
    MultipleInputs.addInputPath(job, new Path(storeFile),TextInputFormat.class,StoreMapper.class);
    FileOutputFormat.setOutputPath(job,new Path(outputDir));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
    
  }
}