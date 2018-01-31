package PIT.Indexing;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import PIT.Component;
import PIT.ProbabilityIntegralTransformation;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import XmlParser.Parser;



public   class IndexMapper extends Mapper<LongWritable, Text,Text, Text> {
     
	ProbabilityIntegralTransformation transofrmation;
	
	double cellWidth;
	
	int totalWidth;
	
	int dimension;
	
	//HashMap<Integer, Component> components;
	@Override
	public void setup(Context context) throws IOException 
	{
		
		//components = new HashMap<Integer, Component>();
		
		 cellWidth = Double.valueOf(context.getConfiguration().get("cellWidth"));
	 
		
		 totalWidth = Integer.valueOf(context.getConfiguration().get("totalWidth"));
		 
		 String pCovariance = context.getConfiguration().get("pCovarainace");
		  
		 String pMean = context.getConfiguration().get("pMean");
		 
		 dimension = Integer.valueOf(context.getConfiguration().get("dimension"));
		 
		 
		 Path cov= new Path(pCovariance);//Location of file in HDFS
		 
		 Path avg = new Path(pMean);
		 
		 FileSystem fs = FileSystem.get(new Configuration());
		 
		
		 
	 
		 
//		 String line;
//		 
//		 int count = 1;
//		 
//		 while((line = br.readLine()) != null)
//		 {
//			    String[] parameters = line.split(" ");
//			 
//			    double[][] covariance = new double[2][2];
//				double[] mean = new double[2];
//							
//				mean[0] = Double.valueOf(parameters[0]);
//				mean[1] = Double.valueOf(parameters[1]);
//				
//				covariance[0][0] = Double.valueOf(parameters[2]);			
//			    covariance[0][1] = Double.valueOf(parameters[3]);			
//			    covariance[1][0] = Double.valueOf(parameters[4]);			
//			    covariance[1][1] = Double.valueOf(parameters[5]);
//			    
//			    double lamda = Double.valueOf(parameters[6]);
//			    
//			    Component cmp = new Component(covariance, mean,lamda);
//			    
//			    components.put(count, cmp);
//			    
//			    count++;
//			 
//		 }
//		 br.close();
//		
//	    double[][] covariance = new double[2][2];
//		
//        covariance[0][0] =  3602183741f;
//		
//	    covariance[0][1] = 18034917;
//		
//	    covariance[1][0] = 18034917;
//		
//	    covariance[1][1] = 3575937808f;
//	    
//	    double[] mean = new double[2];
//	    
//	    mean[0] = 250000.0f;
//	    mean[1] = 250000.0f;
//	    
//	    transofrmation  = new ProbabilityIntegralTransformation(covariance, mean);
		 
		
		 String line;
		 		 
		BufferedReader br =new BufferedReader(new InputStreamReader(fs.open(cov)));
		
		double[][] covariance = new double[dimension][dimension];
		
		double[] mean = new double[dimension];
		
		int rowCount = 0;
		while((line = br.readLine()) != null)
		 {
			 String[] strline = line.split(",");
				
				int colCount =0;
				for(String val : strline)
				{
					covariance[rowCount][colCount]= Double.valueOf(val);
					colCount++;
				}
				
				rowCount++;
			 
		 }
		 br.close();
		 
		 
		 line = "";
		 BufferedReader br1 =new BufferedReader(new InputStreamReader(fs.open(avg)));
			while((line = br1.readLine()) != null)
			 {
				 String[] strline = line.split(",");
					
				   
					int colCount =0;
					for(String val : strline)
					{
						mean[colCount]= Double.valueOf(val);
						
						colCount++;
					}
				 
			 }
			
			 br1.close();
		 
		 transofrmation =  new ProbabilityIntegralTransformation(covariance, mean);
				  
	}
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{         
		String[] strpoint = value.toString().split(",");

		double[] point = new double[dimension];
		
		for(int i =0; i < dimension ; i++)
		{
			point[i] = Double.valueOf(strpoint[i]);
		}
 
			
		context.write(new Text(doTraformation(point)),value ); // insert it into hashmap
 }
	
	
//	public double[] doTransformation(double[] con)
//	{
//		double x = 0.0;
//		double y = 0.0;
//		for (Integer key : components.keySet())
//		{
//				double[] removeCorrelation = components.get(key).standaraizeAndremoveCorrelation(new double[] {Double.valueOf(con[0]),Double.valueOf(con[1])});	
//			
//				x+=components.get(key).pnorm(Double.valueOf(removeCorrelation[0]));
//		 
//				y+=components.get(key).pnorm(Double.valueOf(removeCorrelation[1]));
//				
//		}
//		return new double[]{x,y};
//	}
	
	

	
	public String getAddress(Double x, int domainWidth, double cellWidth)
	{

		double y =  x * domainWidth;

		int z = (int)Math.floor(y / cellWidth);

		//z =z * numberOFCellPerWidth;
		//System.out.println(x +" "+y + " "+z);
		return String.valueOf(z * cellWidth);
	}
	
	public String doTraformation(double[] qry)
	{
		double[] standardNormal = transofrmation.standaraizeAndremoveCorrelation(qry); // removes dependencies and normalises data
		 
		 // uniform distribution
		double[] uniform = new double[dimension];
	   
		String winnerCell = "";
		
		for(int i =0 ; i <dimension ;i++)
		{
			if (i ==0)
			{
				uniform[i] = transofrmation.pnorm(standardNormal[i]);
				winnerCell = getAddress(uniform[i], totalWidth, cellWidth);
				 
			}
			else
			{
				uniform[i] = transofrmation.pnorm(standardNormal[i]);
				winnerCell +=","+getAddress(uniform[i], totalWidth, cellWidth);
			}
		}
		return winnerCell;
	}
 

}