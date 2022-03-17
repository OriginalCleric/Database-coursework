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

/*
*works with store.dat
*/
public static class StoreMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    //eg  path/to/store.dat
    public String path = "";

    public int floor = 0;

    /*
    * if the entry is on the correct floor:
    *   output key: store_sk
    *          value: floor
    */
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }

      String row = value.toString();
      string[] columns = row.split(",");
      string storeSk = columns[0];
      int storeFloor = Interger.parseInt(columns[7]);
      if (storeFloor == this.floor)
      {
        word.set(storeSk);
        context.write(word, new IntWritable(storeFloor));
        
      }
    }
}