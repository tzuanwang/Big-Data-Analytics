import java.io.IOException;
import java.util.Arrays;
import javax.naming.Context;
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class PageRankMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] data = value.toString().split("\\s");

        // sample data: A C J 0.166667
        // number of outLinks: data.length - data[0](source) - data[data.length - 1](PR)
        int out_num = data.length - 2;
        // PR is the last element
        Double PR = Double.parseDouble(data[data.length - 1]);
        Double out_value = PR / out_num;
        String source = data[0];

        // output: C, (A, PR/2)
        for (int i = 1; i < data.length - 1; i++) {
            context.write(new Text(data[i]), new Text (data[0] + "," + out_value));
        }

        // last row: A (C, J)
        String[] out_pages = Arrays.copyOfRange(data, 1, data.length-1);
        context.write(new Text(data[0]), new Text (String.join(",", out_pages)));
    }
}


