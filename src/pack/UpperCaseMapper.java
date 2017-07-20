package pack;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UpperCaseMapper extends Mapper<Text, Text, Text, IntWritable>

{

	
	public void map(Text key, Text value, Context context) throws InterruptedException, IOException
	{
		String word=key.toString().toUpperCase();
		
		System.out.println(word);
		
		context.write(new Text(word), new IntWritable(Integer.parseInt(value.toString())));
	}
}
