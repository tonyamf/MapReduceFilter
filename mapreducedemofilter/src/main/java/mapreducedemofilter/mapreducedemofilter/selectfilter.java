
/**
 * 
 */
package mapreducedemofilter.mapreducedemofilter;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * @author antonio
 *
 */
public class selectfilter {

	  public static class FilterMapper 
      extends Mapper<Object, Text, Text, Text>{
   
   //private final static IntWritable one = new IntWritable(1);
   //private Text word = new Text();
     
   public void map(Object key, Text value, Context context
                   ) throws IOException, InterruptedException {
	   
     //StringTokenizer itr = new StringTokenizer(value.toString());
     String line = value.toString();
     String[] words = line.split(",");
     //FOOD NAME,SCIENTIFIC NAME,GROUP,SUB GROUP
     String wordOut = words[0] + "," + words[2];
       context.write(new Text(wordOut), new Text(""));
   }
 }
 
 /*public static class IntSumReducer 
      extends Reducer<Text,IntWritable,Text,IntWritable> {
   private IntWritable result = new IntWritable();

   public void reduce(Text key, Iterable<IntWritable> values, 
                      Context context
                      ) throws IOException, InterruptedException {
     int sum = 0;
     for (IntWritable val : values) {
       sum += val.get();
     }
     result.set(sum);
     context.write(key, result);
   }
 }*/

 public static void main(String[] args) throws Exception {
   Configuration conf = new Configuration();
   String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
   if (otherArgs.length < 2) {
     System.err.println("Usage: wordcount <in> [<in>...] <out>");
     System.exit(2);
   }
   Job job = Job.getInstance(conf, "");
   job.setJarByClass(selectfilter.class);
   job.setMapperClass(FilterMapper.class);
   //job.setCombinerClass(IntSumReducer.class);
   //job.setReducerClass(IntSumReducer.class);
   job.setNumReduceTasks(0);
   job.setOutputKeyClass(Text.class);
   job.setOutputValueClass(Text.class);
   for (int i = 0; i < otherArgs.length - 1; ++i) {
     FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
   }
   FileOutputFormat.setOutputPath(job,
     new Path(otherArgs[otherArgs.length - 1]));
   System.exit(job.waitForCompletion(true) ? 0 : 1);
 }

}
