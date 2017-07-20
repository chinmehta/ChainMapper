package pack;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> 

{
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws
	 InterruptedException, IOException
	{
		
		int wc=0;
		
		for(IntWritable val: values)
		{
			wc+=val.get();
		}
		
		
		context.write(key,  new IntWritable(wc));
	}
	

}
