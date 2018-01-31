package PIT.Indexing;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class IndexCombiner extends Reducer<Text, Text, Text, Text >
{
 	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
 	{
		
 	   int count = 0;
	   
	   StringBuilder rowValues = new StringBuilder();
	   
	   for (Text value : values) 
		{
		   String point = value.toString();
	   
		   if (count==0)
		   {
			 	rowValues = new StringBuilder(point) ;
				count++;
			}
			else
			{
			    rowValues.append("\t"+ point);
			   count++;
			}
			             
		}
	    
	    
	   context.write(key, new Text(rowValues.toString()));
 	}

}
