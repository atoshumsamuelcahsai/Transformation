package PIT;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Scanner;

import PIT.Indexing.HbaseConnection;

public class PITQryProcessingMain {
	
	static HbaseConnection hb;
	
	//static ProbabilityIntegralTransformation mr;
	
	public static void main(String[] args) throws IOException 
	{
        System.out.println("Time,NumberOfCells,NumberOFDatPoints");
			    
//	    int k = Integer.valueOf(args[0]);
//	    
//	    int width = Integer.valueOf(args[1]);
//	    
//	    String tableName = args[2];
//	    
//	    String inferencePath = args[3];
	    
	    
       int k = Integer.valueOf("100");
	    
	    int width = Integer.valueOf("250");
	    
	    String tableName = "atoshum:4-multinormal";
	    
	    String inferencePath = "/home/atoshum/inference.csv";
	   		
		hb = new HbaseConnection(k, tableName, width, inferencePath);
				
		BufferedWriter bw = new BufferedWriter(new FileWriter("/home/atoshum/"+tableName+"_"+k+".csv") );
			 
		Scanner sc = new Scanner (new File("/home/atoshum/qry.csv"));
	    
		 while (sc.hasNext())
	     {	 
	    	 
	    	 long x =  System.currentTimeMillis();
	    	 
		     double[] results = hb.knnQuery(sc.nextLine());
		     
		     long diff = System.currentTimeMillis() - x;
		     
		     bw.write(String.valueOf(diff)+","+String.valueOf(results[0])+","+ String.valueOf(results[1])+"\n");
        	}
		 
		 sc.close();
		 bw.flush();
		 bw.close();
		 hb.closeTable();
		 
		
	}

}
