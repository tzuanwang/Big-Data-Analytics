import java.io.IOException;
import javax.naming.Context;
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class TotalOrderSortMapper extends Mapper<LongWritable, Text, Text, Text> {
    
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Convert the line to a string
        String line = value.toString();

        // Check if the line is not empty and has at least ten characters
        if (line != null && line.length() >= 10) {
            // Extract the first ten bytes as the sort key
            String sortKey = line.substring(0, 10);
            
            // Output the sort key and the original line as the value
            context.write(new Text(sortKey), new Text(line.substring(10, line.length())));
        }
    }
}



