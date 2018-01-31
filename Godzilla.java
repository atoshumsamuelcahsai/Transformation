package PIT;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;
import java.util.Scanner;

import org.ejml.data.DenseMatrix64F;

import PythiaHbase.AscendingHeapSort;
import PythiaHbase.DataDistance;
import PythiaHbase.Helper;

public class Godzilla {

	  static int dimension = 2;
	   
	 
		public static void main(String[] args) throws IOException 
		{
				Scanner qrysc = new Scanner(new File("/home/atoshum/test"));
		   	
		   	while (qrysc.hasNextLine())
		   	{
		   				   	
		   	double[] qry =  convertoDouble(qrysc.nextLine());
		   		
			Scanner sc = new Scanner(new File("/home/atoshum/iStanbulSample.csv"));
			
		 
			
			AscendingHeapSort knn = new AscendingHeapSort();
			int k = 10; 
			while (sc.hasNext())
			{
				String line =  sc.nextLine();
				
				 double[] point = convertoDouble(line);
				 
				
				double dis = Helper.ecuDis(point, qry);
				
				knn.addNewData(line, dis);
				
				 
			}
			sc.close();
			
			while(! knn.isEmpty() && k >0)
			{
				
				DataDistance data =knn.removeMin();
				
				System.out.println(data.getRowKey()+ " "+ data.getDistance());
				 
				k--;
			}
		   	}
	}
		
		public static void standardize() throws FileNotFoundException
		{
			  String pathcovariance = "/home/atoshum/covariance.csv";
			    String pathmean = "/home/atoshum/mean.csv";
			double[][] covariance = new double[dimension][dimension];
			double[] mean = new double[dimension];

			Scanner coSC = new Scanner(new File(pathcovariance));
			int rowCount = 0;
			while(coSC.hasNextLine())
			{
				String[] line = coSC.nextLine().split(",");

				int colCount =0;
				for(String val : line)
				{
					covariance[rowCount][colCount]= Double.valueOf(val);
					colCount++;
				}

				rowCount++;
			}
			coSC.close();

			//System.out.println("reading covariance ended.....");

			Scanner meanSC = new Scanner(new File(pathmean));
			while(meanSC.hasNextLine())
			{
				String[] line = meanSC.nextLine().split(",");
				int colCount =0;
				for(String val : line)
				{
					mean[colCount]= Double.valueOf(val);

					colCount++;
				}
			}
			meanSC.close();


			//System.out.println("reading mean ended......");

			 
		}
		
		
		public  static double[] convertoDouble(String strLine)
		{
			String[] strPoint = strLine.split(",");
			double[] doublePoint = new double[dimension];
			for (int i = 0; i < dimension; i++ )
			{
				doublePoint[i] = Double.valueOf(strPoint[i]);
			}
			return doublePoint;
		}

}
