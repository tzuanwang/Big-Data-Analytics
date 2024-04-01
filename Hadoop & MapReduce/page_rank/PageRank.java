import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;

public class PageRank {
    public static void main(String[] args) throws Exception { 
      for (int i = 0; i < 3; i++) {
        Configuration config = new Configuration();
        config.set("mapred.textoutputformat.separator", " ");
        
        Job job = Job.getInstance();
        job.setJarByClass(PageRank.class);  
        job.setJobName("Page Rank");
        
        FileInputFormat.addInputPath(job, new Path(args[i]));
        FileOutputFormat.setOutputPath(job, new Path(args[i+1]));
        job.setMapperClass(PageRankMapper.class);
        job.setReducerClass(PageRankReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
  
        job.setNumReduceTasks(1);
        
        job.waitForCompletion(true);
  
        if (i == 2) {
          System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
      } 
    }
}

