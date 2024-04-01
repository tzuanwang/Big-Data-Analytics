import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;

public class TotalOrderSort {
    public static void main(String[] args) throws Exception { 
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        Path stagingPath = new Path(args[1] + "_staging");
        Path partitionFile = new Path(args[1] + "_partition");
      
        // Configure job to prepare for sampling
        Job sampleJob = Job.getInstance();
        sampleJob.setJarByClass(TotalOrderSort.class);
        
        // Use the mapper implementation with zero reduce tasks 
        sampleJob.setMapperClass(TotalOrderSortMapper.class); 
        sampleJob.setNumReduceTasks(0);

        sampleJob.setOutputKeyClass(Text.class);
        sampleJob.setOutputValueClass(Text.class);

        TextInputFormat.setInputPaths(sampleJob, inputPath);

        // Set the output format to a sequence file 
        sampleJob.setOutputFormatClass(SequenceFileOutputFormat.class); 
        SequenceFileOutputFormat.setOutputPath(sampleJob, stagingPath);

        // Submit the job 
        sampleJob.waitForCompletion(true); 


        Job orderJob = Job.getInstance();
        orderJob.setJarByClass(TotalOrderSort.class);

        // Here, use the identity mapper to output the key/value pairs in the SequenceFile 
        orderJob.setReducerClass(TotalOrderSortReducer.class);

        // Set the number of reduce tasks to an appropriate number for the amount of data being sorted 
        orderJob.setNumReduceTasks(10);

        // Use Hadoop's TotalOrderPartitioner class 
        orderJob.setPartitionerClass(TotalOrderPartitioner.class);


        // Set the partition file 
        TotalOrderPartitioner.setPartitionFile(orderJob.getConfiguration(), partitionFile);
  
        orderJob.setOutputKeyClass(Text.class);
        orderJob.setOutputValueClass(Text.class);

        // Set the input to the previous job's output 
        orderJob.setInputFormatClass(SequenceFileInputFormat.class); 
        SequenceFileInputFormat.setInputPaths(orderJob, stagingPath);

        // Set the output path to the command line parameter 
        TextOutputFormat.setOutputPath(orderJob, outputPath);

        // Use the InputSampler to go through the output of the previous job, sample it, and create the partition file 
        InputSampler.writePartitionFile(orderJob, new InputSampler.RandomSampler(.001, 10000));

        // Submit the job
        orderJob.waitForCompletion(true);
    }
}

