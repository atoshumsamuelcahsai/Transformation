package PIT.Indexing;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;

import PIT.Component;

public class TestingClusters {
	
	
static HashMap<String ,ArrayList<String>> cont = new HashMap<String ,ArrayList<String>>();
	
	static HashMap<Integer, Component> components = new HashMap<Integer, Component>();
	
	public static void main(String[] args) throws IOException 
	{
		readInferedParameters();
		
		int count = 0;
		while(count <=12){
			Scanner sc = new Scanner(new File("/home/atoshum/Clusters/"+count+".csv"));
			BufferedWriter bw = new BufferedWriter(new FileWriter("/home/atoshum/Uniform/"+count+".csv"));
			while(sc.hasNextLine())
			{
				String[] line = sc.nextLine().split(",");
				
				double[] con = new double[]{Double.valueOf(line[0]), Double.valueOf(line[1])};
			    double[] removeCorrelation = components.get(count).standaraizeAndremoveCorrelation(con);
				double x =components.get(count).pnorm(Double.valueOf(removeCorrelation[0]));
				double y =components.get(count).pnorm(Double.valueOf(removeCorrelation[1]));
				//System.out.println(x +","+y);
				bw.write(String.valueOf(x)+","+String.valueOf(y)+"\n");
				
			}
			bw.flush();
			bw.close();
			sc.close();
			count++;
			
		}
		
		read();
		BufferedWriter bwuniform = new BufferedWriter(new FileWriter("/home/atoshum/uniform.csv"));
		int countKey =0;
		int countValue=0;
		for (String key : cont.keySet())
		{
			countKey ++;
			for (String value : cont.get(key))
			{
				countValue++;
			}
			System.out.println( cont.get(key).size());
			
			bwuniform.write(key +","+String.valueOf(cont.get(key).size())+"\n");
			
			 
		}
		bwuniform.flush();
		bwuniform.close();
	}
	
	public static void readInferedParameters() throws FileNotFoundException
	{
 		Scanner sc = new Scanner(new File("/home/atoshum/inference.csv"));
		 int count = 0;
		double[][] test = new double[2][2];
		while (sc.hasNextLine())
		{
			 
			//System.out.println(count);
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
 	  	    components.put(count, cmp);
		     count++;
		}
		 
		
		sc.close();
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
	public static String getAddress(Double x)
	{
		 
		double y =  x * 49;
		
		int z = (int)y /7;
		
		z =z * 7 ;
		//System.out.println(x +" "+y + " "+z);
		return String.valueOf(z);
	}
	
	public static void read() throws FileNotFoundException
	{
		Scanner sc = new Scanner(new File("/home/atoshum/Uniform/0.csv"));
		while(sc.hasNextLine())
		{
			String[] line = sc.nextLine().split(",");
			
			double[] con = new double[]{Double.valueOf(line[0]), Double.valueOf(line[1])};
			
			add(getAddress(con[0])+"-"+getAddress(con[1]),con[0]+"-"+con[1] ); // insert it into hashmap
		}
	  sc.close();
	}

}
