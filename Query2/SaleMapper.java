package Query2;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SaleMapper {
    public static class MyMapper extends Mapper<Object, Text, Text, DoubleWritable>{
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String startDate = conf.get("startDate");
            String endDate = conf.get("endDate");
            
            StringTokenizer itr = new StringTokenizer(value.toString());
                while (itr.hasMoreTokens()) {
                    word.set(itr.nextToken());
                    String s = word.toString();
                    String[] array = s.split("\\|");

                    if (array[7].length() == 0 || array.length < 21 || array[0].length() == 0 
                    || Integer.parseInt(array[0]) < Integer.parseInt(startDate) || Integer.parseInt(array[0]) > Integer.parseInt(endDate)
                    ){
                        continue;
                    }
                    if (array[20].length() == 0){
                        array[20] = "0";
                    }
                    context.write(new Text(array[7]), new DoubleWritable(Float.parseFloat(array[20])));
                }
            }
    }

}