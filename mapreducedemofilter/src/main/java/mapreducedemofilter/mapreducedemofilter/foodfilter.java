package mapreducedemofilter.mapreducedemofilter;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable; // for inwritable
import org.apache.hadoop.io.Text; //For the text input and output
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper; // imported for the Mapper
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class foodfilter {

	public static class ToekenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
		
		/**
		 * Context will intermidia value or output
		 * @throws InterruptedException 
		 * @throws IOException 
		 */
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer stIn = new  StringTokenizer(value.toString());
			Text wordOut = new Text();
			IntWritable one = new IntWritable(1);
			while(stIn.hasMoreTokens()) {
				wordOut.set(stIn.nextToken());
				context.write(wordOut,one);
			}
		}
		
		
	} 
	
	public static class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		
		
		public void reduce(Text term, Iterable<IntWritable> ones, Context context) throws IOException, InterruptedException {
			int count = 0;
			Iterator<IntWritable> iterator = ones.iterator();
			while(iterator.hasNext()) {
				count++;
				iterator.next();
			}
			IntWritable output = new IntWritable(count);
			context.write(term, output);
			
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if(otherArgs.length != 2) {
			System.err.println("Usage: foodfilter <input_dir/file> <output_dir>");
			System.exit(2);//difrente zero 0 erro hadoop
		}
		
		Job job = Job.getInstance(conf, "food filter");
		job.setJarByClass(foodfilter.class);
		job.setMapperClass(ToekenizerMapper.class);
		job.setReducerClass(SumReducer.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		boolean satus = job.waitForCompletion(true);
		if(satus){
			System.exit(0);
		}else {
			System.exit(1);
		}
	}

}
