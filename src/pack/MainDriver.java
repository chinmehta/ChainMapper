package pack;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MainDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
	
		if(args.length != 3)
		{
			
			System.out.println("You haven't provided input and output dir");
		}
		
		Job job=new Job();
		job.setJarByClass(MainDriver.class);
		
		job.setJobName("Chaining of Mappers");
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(ChainMapper1.class);
		job.setNumReduceTasks(0);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.waitForCompletion(true);
		
		/* Job 2*/
		
		
		
		Job job2=new Job();
		
		job2.setJarByClass(MainDriver.class);
		job2.setJobName("Second Job");
		
		job2.setMapperClass(UpperCaseMapper.class);
		job2.setReducerClass(WordCountReducer.class);
		
		//We Have to provide InputFormat for second job because first job have emitted the
		// data in the key-value format
		job2.setInputFormatClass(KeyValueTextInputFormat.class);
		
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(IntWritable.class);
		
		System.out.println("Second Job");
		
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);
		
		
		// Input Path(Output dir emitted by first job)
		FileInputFormat.setInputPaths(job2, new Path(args[1]));
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));
		
		
		boolean success=job2.waitForCompletion(true);
		return success ? 0 : 1;
		
		
		
	
		
		
		
	}

	public static void main(String args[]) throws IOException, Exception
	{
		Configuration conf=new Configuration();
		int res=ToolRunner.run(conf, new MainDriver(), args);
		
		System.exit(res);
	}
	
}
