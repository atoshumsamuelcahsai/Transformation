package PIT.Indexing;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Scanner;

import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.EigenDecomposition;
import org.apache.commons.math3.linear.LUDecomposition;
import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;

import PIT.Component;
import PythiaHbase.AscendingHeapSort;

public class MultivariatePIT 
{
	static HashMap<String ,ArrayList<String>> cont = new HashMap<String ,ArrayList<String>>();
	
	static HashMap<Integer, Component> components = new HashMap<Integer, Component>();
	
    static int totalWidth = 300;
	
	static int cellWidth = 100;
	
    static AscendingHeapSort knn;
	
	static int valueOFK = 619074;
	
	static HashSet<String> hs;
	 
	static AscendingHeapSort tempknn;
	
	static double[] meanhat;
	
	static RealMatrix sqrtOfInverseCovarianceMatrix  ;
	
	public static void main(String[] args) throws IOException 
	{
		
	readInferedParameters();

	
	
	BufferedWriter bw = new BufferedWriter(new FileWriter("/home/atoshum/index.csv"));
	
	BufferedWriter bwuniform = new BufferedWriter(new FileWriter("/home/atoshum/uniform.csv"));
	int count = 0;
	
	while(count <= 8)
	{
	Scanner sc = new Scanner(new File("/home/atoshum/Clusters/"+count+".csv"));
	
	while (sc.hasNext())
	{
		String line =  sc.nextLine();
				
		String[] con = line.split(",");
				
		double[] pit = doTransformation(new double[]{Double.valueOf(con[0]), Double.valueOf(con[1])});
		
		bwuniform.write(String.valueOf(pit[0])+","+String.valueOf(pit[1])+"\n");
		
		
		add(getAddress(pit[0],totalWidth, cellWidth)+"-"+getAddress(pit[1],totalWidth, cellWidth),con[0]+"-"+con[1] ); // insert it into hashmap
		
		 
	}
	count +=2;
	sc.close();
	}
	bwuniform.close();
	
	
	
	int countKey =0;
	int countValue=0;
	for (String key : cont.keySet())
	{
		countKey ++;
		for (String value : cont.get(key))
		{
			countValue++;
		}
		System.out.println(key + " "+ cont.get(key).size());
		
		bw.write(key +","+String.valueOf(cont.get(key).size())+"\n");
		
		 
	}
	System.out.println("Total key "+ countKey + " "+ "Total value "+ countValue);
	bw.close();
	 
	 
}
	public static void add(String key, String data)
	{
		if (!cont.containsKey(key))
		{
			ArrayList<String> a = new ArrayList<String>();
			a.add(data);
			cont.put(key, a);
		}
		else
		{
			cont.get(key).add(data);
		}
	}	
	 
	
	public static void readInferedParameters() throws FileNotFoundException
	{
		double[][] c_i = new double[2][2];
		
		meanhat = new double[2];
				
		Scanner sc = new Scanner(new File("/home/atoshum/inference.csv"));
		
		int count = 1;
		
		
				
		while (sc.hasNextLine())
		{
			if ((count % 2) != 0) 
				{
				 count++;
				  continue;
				}
			String[] parameters = sc.nextLine().split(" ");
			
			double[][] covariance = new double[2][2];
			
			double[] mean = new double[2];
						
			mean[0] = Double.valueOf(parameters[0]);
			mean[1] = Double.valueOf(parameters[1]);
			
			covariance[0][0] = Double.valueOf(parameters[2]);			
		    covariance[0][1] = Double.valueOf(parameters[3]);			
		    covariance[1][0] = Double.valueOf(parameters[4]);			
		    covariance[1][1] = Double.valueOf(parameters[5]);
		    
		    double lamda = Double.valueOf(parameters[6]);
		    
		    Component cmp = new Component(covariance, mean,lamda);
		    	    
		    c_i[0][0] += (covariance[0][0]   * lamda);			
		    c_i[0][1] += (covariance[0][1]   * lamda);			
		    c_i[1][0] += (covariance[1][0]   * lamda);			
		    c_i[1][1] += (covariance[1][1]   * lamda);
		    
		    meanhat[0] += (mean[0] * lamda);
		    meanhat[1] += (mean[1] * lamda);
		    	    
		    components.put(count, cmp);
		     count++;
		}
			
		
		RealVector mean_hat = new ArrayRealVector(meanhat);
		RealMatrix covvarince_hat = MatrixUtils.createRealMatrix(new double[2][2]);
		for(int key : components.keySet())
		{
				RealVector mean_i = new ArrayRealVector(components.get(key).getMean());
				
				RealVector x = mean_i.subtract(mean_hat);
							 				
 				covvarince_hat = covvarince_hat.add(x.outerProduct(x).scalarMultiply(components.get(key).getLamda()));
 
 	}
 
 		
 		RealMatrix z = covvarince_hat.add( MatrixUtils.createRealMatrix(c_i));
 
		 
		
		sqrtOfInverseCovarianceMatrix = new EigenDecomposition(new LUDecomposition(z).getSolver().getInverse()).getSquareRoot();
		
		
		 
		
		sc.close();
	}
	
public static double[] doTransformation(double[] con) throws IOException
{
	double x = 0.0;
	double y = 0.0;
	double[] removeCorrelation = standaraizeAndremoveCorrelation(con);
	for (Integer key : components.keySet())
	{
		   // double[] removeCorrelation = components.get(key).standaraizeAndremoveCorrelation(con);
			x+=components.get(key).pnorm(Double.valueOf(removeCorrelation[0]));
			y+=components.get(key).pnorm(Double.valueOf(removeCorrelation[1]));
	}
	return new double[]{x,y};
}
	

	
	public static String getAddress(Double x, int width, int numberOFCellPerWidth)
	{
		//System.out.println("++++++++++++++++++++++++++++++++");
		double y =  x * width;
		
		int z = (int)y / numberOFCellPerWidth;
		
		z =z * numberOFCellPerWidth;
		//System.out.println(x +" "+y + " "+z);
		return String.valueOf(z);
	}
	
	
	public static double[] standaraizeAndremoveCorrelation(double[] point) throws IOException
	{
		
		for (int i = 0; i < meanhat.length; i++)
		{
			point[i]  =  (point[i] - meanhat[i]);
		}
		//double[] x = sqrtOfInverseCovarianceMatrix.operate(point);
				 
		return sqrtOfInverseCovarianceMatrix.operate(point);
	}
}
