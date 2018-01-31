package PIT.Indexing;
 
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
 
import org.mortbay.log.Log;

import XmlParser.Parser;
 


public class IndexReducer extends Reducer<Text, Text, Text, Text >{

	
	private MultipleOutputs<IntWritable,Text> twolevel;
	private MultipleOutputs<IntWritable,Text> index;
	
	 
	@Override
	public void setup(Context context) 
	{
		this.twolevel =   new MultipleOutputs(context);
		
		this.index = new MultipleOutputs(context);
		
	 
		 
	}
	 
 //	protected void reduce(Text key, Iterable<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException, InterruptedException 
 	protected void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException
	{
		
 	   int count = 0;
	   
	   StringBuilder rowValues = new StringBuilder();
	   boolean firstKey = true;
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
	   
	   this.index.write(new IntWritable(count),key ,"head_index_file");
	   
 
      
        
	}
	@Override
 	protected void cleanup(Context context) throws IOException,InterruptedException
    {
//		this.twolevel.close();
		this.index.close();
		
		 
	}
 	






	


}
