import java.io.IOException;
import javax.naming.Context;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TotalOrderSortReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) 
                       throws IOException, InterruptedException {
        // Iterate through all values associated with the key and write them to the context
        for (Text value : values) {
            context.write(key, value);
        }
    }
}


