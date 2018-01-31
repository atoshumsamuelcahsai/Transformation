package PIT;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Scanner;

import PythiaHbase.AscendingHeapSort;
import PythiaHbase.DataDistance;
import PythiaHbase.Helper;

public class TestOnlineIndexing 
{
	static HashMap<String ,double[]> cont = new HashMap<String ,double[]>();
	
	static double [][] inverse = null;
	
	static ProbabilityIntegralTransformation mr;
		
	static int totalWidth = 478780;
	
	static int cellWidth = 100000;
	
	static AscendingHeapSort knn;
	
	static int valueOFK = 10;
	
	static double totalData ;
	
	static HashSet<String> hs;
	 
	static AscendingHeapSort tempknn;
	
	static double pOfX ;
	
	public static void main(String[] args) throws IOException 
	{
        
		 
		
		double[][] covariance = new double[2][2];
		
		covariance[0][0] =  3602183741f;
		
	    covariance[0][1] = 18034917;
		
	    covariance[1][0] = 18034917;
		
	    covariance[1][1] = 3575937808f;
	    
	    double[] mean = new double[2];
	    
	    mean[0] = 250000.0f;
	    mean[1] = 250000.0f;
	    
	    mr = new ProbabilityIntegralTransformation(covariance, mean);
	   		
		Scanner sc = new Scanner(new File("/home/atoshum/multivarTest.csv"));
		
		BufferedWriter bw = new BufferedWriter(new FileWriter("/home/atoshum/uniform.csv"));
		
		while (sc.hasNext())
		{
			String line =  sc.nextLine();
			
			//System.out.println(line);
			
			String[] con = line.split(",");
			
			//System.out.println(con.length);
			
			double[] independent = mr.standaraizeAndremoveCorrelation(new double[] {Double.valueOf(con[0]),Double.valueOf(con[1])});
			
			 
			String x_address =  String.valueOf(mr.pnorm(independent[0]) * totalWidth );
			
			String y_address = String.valueOf(mr.pnorm(independent[1]) * totalWidth );
			
			//bw.write(String.valueOf(mr.pnorm(Double.valueOf(independent[0])))+","+String.valueOf(mr.pnorm(independent[1]))+"\n");
			
			
			add(x_address+"-"+y_address, new double[] {Double.valueOf(con[0]),Double.valueOf(con[1])} ); // insert it into hashmap
			 
		}
		sc.close();
		
		
		totalData = cont.size();
		
		System.out.println( valueOFK +" "+ cont.size() + " " + totalWidth + " " +pOfX);
	 
		knnTest("195944.4644504353,219121.906149015");
		
		
	}
	
 
	
	public static void add(String key, double[] data)
	{
		 	 
			cont.put(key,data);
		 
	}
	

	
	public static void knnTest(String query)
	{
		knn = new AscendingHeapSort(); // heap sort
		 
		String[] strQry = query.split(",") ;
		
		double[] qry = new double[]{Double.valueOf(strQry[0]),Double.valueOf(strQry[1])};  // qry in double[]
		
		double[] independent = mr.standaraizeAndremoveCorrelation(new double[] {Double.valueOf(strQry[0]),Double.valueOf(strQry[1])}); // removes dependencies and normalises data
		
	   double cellWidth = totalData / getTotaNumberOFCells();
		
		 double qryXMin = (mr.pnorm(independent[0]) * totalWidth )  - (4 * cellWidth); // get address for  x dimension based on CDF of x
		 double qryXMax = (mr.pnorm(independent[0]) * totalWidth )  + ( 4 * cellWidth); // get address for  x dimension based on CDF of x
		 //System.out.println(qryXMin +" "+qryXMax+ " "+ pOfX);
		
		 double qryYMin = (mr.pnorm(independent[1]) * totalWidth)   - (4 * cellWidth);  // get address for y dimension based on CDF of y
		 double qryYMax = ( mr.pnorm(independent[1]) *totalWidth)   + (4 * cellWidth);  // get address for y dimension based on CDF of y
		 
		// System.out.println(qryXMin + " "+ qryXMax + " "+ qryYMin + " "+ qryYMax) ;
		 
	 
		 
		 for (String key : cont.keySet())
		 {
			//System.out.println(key);
			 String[] strKey = key.split("-");
			 double x = Double.valueOf(strKey[0]);
			 double y = Double.valueOf(strKey[1]);
	
			 if ( ( x <= qryXMax) && ( x >= qryXMin)  && ( y <= qryYMax) && ( y >= qryYMin) )
			 {
				//	System.out.println( x +","+qryXMax +","+ qryXMin+","+ y+","+qryYMax+","+ qryYMin);
					double[] data = cont.get(key);
					
					double dis = Helper.ecuDis(data, qry);
					String value = String.valueOf(data[0]) + ","+ String.valueOf(data[1]);
					knn.addNewData(value, dis);
			 }
			 
		 }
		 
		
	
	
		
		
	
		
		System.out.println(knn.size());
		int k = valueOFK;
		while(! knn.isEmpty() && k >0)
		{
			
			DataDistance data =knn.removeMin();
			
			System.out.println(data.getRowKey() + " "+ data.getDistance());
			 
			k--;
		}
				
		
	}
	
	public static double getTotaNumberOFCells( )
	{
		return Math.sqrt((double)totalData/ (double)valueOFK);
	}
	
	 
}
