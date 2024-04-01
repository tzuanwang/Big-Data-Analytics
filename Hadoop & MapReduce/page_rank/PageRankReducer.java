import java.io.IOException;
import javax.naming.Context;
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Double TotalPageRank = 0.0;
        String OutValue = "";
        String OutKey = "";

        // Iterate through the key-value pairs and split them to add up
        for (Text elements: values) {
        String [] elements_val = elements.toString().split(",");
        
        //Check whether it's Double
        boolean numeric = true;
        try {
            Double PageRank_val = Double.parseDouble(elements_val[1]);
        } catch (Exception e) {
            numeric = false;
        }
        
        //True - Sum up
        if (numeric) {
            Double PageRank_val = Double.parseDouble(elements_val[1]);
            TotalPageRank += PageRank_val;
        }
        //False - add to elements_val 
        else {
            OutKey = key.toString();
            OutValue = String.join(" ", elements_val);
        }
        }
        // final output should be the same as input file: (A C J 0.166667)
        context.write(new Text (OutKey), 
                      new Text(OutValue + " " + String.valueOf(TotalPageRank)));
    }
}


