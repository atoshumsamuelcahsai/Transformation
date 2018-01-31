package PIT;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Scanner;

import PIT.Indexing.HbaseConnection;
import PIT.Indexing.HbaseConnectionVersion2;

public class PITQryProcessingMainVersion2 implements java.io.Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	static HbaseConnectionVersion2 hb;
	
	//static ProbabilityIntegralTransformation mr;
	
	public static void main(String[] args) throws IOException 
	{
        System.out.println("Time,NumberOfCells,NumberOFDatPoints");
			    
//	    int k = Integer.valueOf(args[0]);
//        double cellWidth = Double.valueOf(args[1]);
//        int domainwidth = Integer.valueOf(args[2]);
//	    String tableName = args[3];
//        String covariance = args[4];
//	    String mean  = args[5];
//        String outPutPath = args[6];
//        String qry = args[7];
//        int dimension = Integer.valueOf(args[8]);
 
 	   
        
        int dimension = 6;
	    int k = Integer.valueOf("10");
        double cellWidth = Double.valueOf("13");
	    int domainwidth = Integer.valueOf("169");
	    String tableName = "atoshum:activties";
	    String covariance = "/home/atoshum/Dropbox/DatasetFOrPIT/AReM/covarianceCyclyingandwalkingTogether.csv";
	    String mean = "/home/atoshum/Dropbox/DatasetFOrPIT/AReM/meanCyclyingandWalkingTogther.csv";
	    String outPutPath= "/home/atoshum/"+tableName+"-"+k+".csv";
	    String qry = "/home/atoshum/Dropbox/DatasetFOrPIT/QueryProcessingTime/data-activities/qry.csv";
	   		
		hb = new HbaseConnectionVersion2(k, tableName, cellWidth,domainwidth, covariance, mean,dimension, outPutPath);
				
		BufferedWriter bw = new BufferedWriter(new FileWriter("/home/atoshum/results/"+tableName+"_"+k+".csv") );
			 
		Scanner sc = new Scanner (new File(qry));
	    int count =0;
	    bw.write("Time"+","+"NumberOfCells"+","+ "NumberOFDatPoints"+"\n");
		 while (sc.hasNext())
	     {	 
			 if (count > 300 ) break;
	    	 long x =  System.currentTimeMillis();
	    	 
	    	 String line  = sc.nextLine();
	    	 
	    	 
		     double[] results = hb.knnQuery(line);
		     
		     long diff = System.currentTimeMillis() - x;
		     
		    bw.write(String.valueOf(diff)+","+String.valueOf(results[0])+","+ String.valueOf(results[1])+"\n");
		     
		     
		     count++;
		      System.out.println(count );;
        	}
		 
		 sc.close();
		 bw.flush();
		 bw.close();
		 hb.closeTable();
		 
		
	}

}
