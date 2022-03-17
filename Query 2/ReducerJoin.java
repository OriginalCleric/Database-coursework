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


public static class ReduceJoin
    extends Reducer<Text,Text,Text,Text> {

    //TODO
    /*
    *aggregate the net paid
    */
    public void reduce(Text key, Iterable<Text> values,
                    Context context
                    ) throws IOException, InterruptedException {
    Double sum = 0;
    string floor;
    for (Text val : values) {
        string[] parts = val.toString().split(",");
        if (parts[0] == "FLOOR")
        {
            floor = parts[1];
        }
        else if (parts[0] == "")
        {
            sum += Double.parseDouble(parts[1]);
        }
    }

    Text result = new Text(sum.toString()+","+floors);
    context.write(key, result);
    }
}