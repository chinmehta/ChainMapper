package pack;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ChainMapper1 extends Mapper<LongWritable, Text, Text, IntWritable>

{

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
 
		IntWritable one=new IntWritable(1);
		
		Text wc=new Text();
		String line=value.toString();
		
		StringTokenizer st=new StringTokenizer(line," ");
		
		while(st.hasMoreTokens())
		{
			
			String word=st.nextToken();
			
			wc.set(word);
			
			context.write(wc, one);
		}
		
	}
	
}
