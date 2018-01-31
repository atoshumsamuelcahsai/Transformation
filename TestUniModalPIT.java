package PIT;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;

import PythiaHbase.AscendingHeapSort;
import PythiaHbase.DataDistance;
import PythiaHbase.DescendingHeapSort;
import PythiaHbase.Helper;

public class TestUniModalPIT 
{
	static HashMap<String ,ArrayList<String>> cont = new HashMap<String ,ArrayList<String>>();

	static double [][] inverse = null;

	static ProbabilityIntegralTransformation mr;

	static int totalWidth =15;    // total width/height (square is assumed here) of the domain space

	static int totalDataPoints = 100000;

	//static int cellWidth = 100000;      // a width/height of a cell

	static AscendingHeapSort knn;		// Heapsort that stored a kNN answer

	static int valueOFK = 15;			// k = 10,100, 1000 ....

	static HashSet<String> hs;         // stored keys(row keys)

	static DescendingHeapSort tempknn;

	static int dim = 6;

	public static void main(String[] args) throws IOException 
	{

		double cellWidth = 5;

		double[][] covariance = new double[dim][dim];

		Scanner coSC = new Scanner(new File("/home/atoshum/covarianceCyclyingandwalkingTogether.csv"));

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


		double[] mean = new double[dim];
		Scanner meanSC = new Scanner(new File("/home/atoshum/meanCyclyingandWalkingTogther.csv"));
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

		mr = new ProbabilityIntegralTransformation(covariance, mean);

		Scanner sc = new Scanner(new File("/home/atoshum/complete.csv"));

		BufferedWriter bw = new BufferedWriter(new FileWriter("/home/atoshum/uniform.csv"));

		while (sc.hasNext())
		{
			String line =  sc.nextLine();

			String address = getCellKey(line,cellWidth);
			//System.out.println(address + " "+line);

			add(address,line ); // insert it into hashmap

			//bw.write(String.valueOf(mr.pnorm(Double.valueOf(independent[0])))+","+String.valueOf(mr.pnorm(independent[1]))+"\n");


			//System.out.println(x_address+","+y_address+"======data"+con[0]+","+con[1]);

		}
		sc.close();

		int countKey =0;
		int countValue=0;
		for (String key : cont.keySet())
		{
			countKey ++;
			for (String value : cont.get(key))
			{
				countValue++;
			}
			// System.out.println(key + " "+ cont.get(key).size());

			bw.write(countKey + ","+String.valueOf(cont.get(key).size())+"\n");
		}
		//System.out.println("Total key "+ countKey + " "+ "Total value "+ countValue);
		bw.close();
		//System.out.println(cellWidth + "\t" + cellWidth / totalWidth);



		//knnTest("25.75,6.02,13.75,2.05,16,1.58", cellWidth);
		//2.18135998086973,3.42723707786645
	}

	public static String getAddress(Double x, int width, double numberOFCellPerWidth)
	{

		double y =  x * width;

		int z = (int)Math.floor(y / numberOFCellPerWidth);

		//z =z * numberOFCellPerWidth;
		//System.out.println(x +" "+y + " "+z);
		return String.valueOf(z * numberOFCellPerWidth);
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

		
	public static  void knnTest(String query, double cellWidth)
	{
		hs = new HashSet<String>(); // the first n cell that contains K number of elements are inserted her. So no need to retrieve them twice

		knn = new AscendingHeapSort(); // heap sort   

		tempknn = new DescendingHeapSort(valueOFK);

		String winnerCell =  getCellKey(query, cellWidth);
				
		hs.add(winnerCell); // the query in the uniform space
 
		double radius = -1;

		int k = valueOFK; 
	
		
		addToKNNCandidate(winnerCell,convertoDouble(query));//initial KNN aswere
		
		//System.out.println("winner "+winnerCell  +" ==== === === = ="+ tempknn.size());
				
		if(! tempknn.isEmpty() && k <= tempknn.size() )//calculate radius
		{
			DataDistance data = tempknn.removeMaximum();
			//knn.addNewData(data.getRowKey(), data.getDistance());
			 radius = data.getDistance() ; // k_th distance
			 
		}
		//System.out.println(radius + " radiusss......."+k);
		
		double[] qry = convertoDouble(query); 
			     
	    ArrayList<String> coordiantes;
	    
	    double[] tranformedUniformMinimum = new double[dim];
	    double[] tranformedUniformMaximum = new double[dim];
	    for(int i = 0 ; i < dim; i++)
	    {
	    	tranformedUniformMinimum[i] = qry[i]- radius;
	    	tranformedUniformMaximum[i]=  qry[i] + radius;
	    	//System.out.println(tranformedUniformMinimum[i] + " == == == "+tranformedUniformMaximum[i]);
	    }
	   
	    tranformedUniformMinimum = getTranformedCoordinates(tranformedUniformMinimum, cellWidth/totalWidth);
	    tranformedUniformMaximum = getTranformedCoordinates(tranformedUniformMaximum, cellWidth/totalWidth);
//	    System.out.println(tranformedUniformMinimum[0] + " == ?== == "+tranformedUniformMaximum[0]);
//	    System.out.println(tranformedUniformMinimum[1] + " ==? == == "+tranformedUniformMaximum[1]);
//	    System.out.println(tranformedUniformMinimum[2] + " == == == "+tranformedUniformMaximum[2]);
	    
	    if (qry.length <= 2)
	    {
	    	coordiantes = getFirstDimesions(new double[]{tranformedUniformMinimum[0], tranformedUniformMaximum[0]}, new double[]{tranformedUniformMinimum[1], tranformedUniformMaximum[1]}, cellWidth/totalWidth);
        }
	    else
	    {
	    	coordiantes = getFirstDimesions(new double[]{tranformedUniformMinimum[0], tranformedUniformMaximum[0]}, new double[]{tranformedUniformMinimum[1], tranformedUniformMaximum[1]},cellWidth/totalWidth);
//	    	for(String x : coordiantes)
//	    	{
//	    		System.out.println(x + " check eight things");
//	    	}
	    	for (int i = 2; i < qry.length; i++)
	    	{
	    		coordiantes =	getFirstDimesions(coordiantes, new double[]{tranformedUniformMinimum[i], tranformedUniformMaximum[i]}, cellWidth/totalWidth );
	    	    
	    	}
	    }
	   for(String key : coordiantes)
	   {
		  
		   hs.add(getCellKeyForTransformedCoordinates(key,cellWidth));
	   }

		int total = 0;
		for(String key : hs)
		{
			 if (winnerCell.compareTo(key)==0) continue;		
			for (String value : cont.get(key))
			{
				double[] x = convertoDouble(value);
				double dis = Helper.ecuDis( x, qry);
				tempknn.addNewData(value, dis);
				total ++;
			}
		}

		System.out.println(knn.size() + " "+valueOFK +" "+ hs.size() + " ====== ===== ====== "+total ) ;
		
		int k1 =  valueOFK;
		
		while(! tempknn.isEmpty())
		{

			DataDistance data =tempknn.removeMaximum();
			knn.addNewData(data.getRowKey() , data.getDistance());

			//System.out.println(data.getRowKey() + " "+ data.getDistance());

			 
		}
		 
		while(!knn.isEmpty() && k1 > 0)
		{

			DataDistance data =knn.removeMin();

			System.out.println(data.getRowKey() + " "+ data.getDistance());

			k1--;
		}



	}

	/*
	 * adds candidate points that resides in cells that overlaps 
	 *  the range query along with distance of the points distance from a query
	 */
	public static int addToKNNCandidate(String key, double[] qry)
	{
		int count =0;
 
		//if (!cont.containsKey(key)) return 0; //============================================================================
		for (String value : cont.get(key))
		{			


			double[] x = convertoDouble(value)  ;


			double dis = Helper.ecuDis(x, qry);

			tempknn.addNewData(value, dis);

			count++;

		}
		 System.out.println(count + " i am count === ++++ ==== ++++++ ==== +++++ ++ ===== +++ +++ === === ");
		return count;

	}

	public static double getCellWidth(double totalDataElements, double dataElePerCell, int dimension)
	{
		double toCellNumber = totalDataElements / dataElePerCell;

		double cellPerDimention =  Math.pow(toCellNumber , 1.0/dimension);

		double cellWidth = 1.0 / cellPerDimention;
		
		return cellWidth;
	}

	public static String[] getKeyConcatenated(String[] p1, String[] p2)
	{

		int count = 0;
		String[]  cardinatlity = new String[p1.length * p2.length];
		for(int i = 0; i < p1.length; i++)
		{
			for(int j=0; j <p2.length; j++)
			{
				cardinatlity[count]= p1[i]+","+p2[j];
				count++;
				//System.out.println(p1[i]+","+p2[j]);
			}

		}

		return cardinatlity;
	}
	public static String getCellKey(String strPoint,double cellWidth)
	{

		double[] qry = convertoDouble(strPoint) ;  // qry in double[]

		return getTransformedAddress(qry, cellWidth);
	}
	
	
	public static String getCellKeyForTransformedCoordinates(String strPoint,double cellWidth)
	{

		double[] uniform = convertoDouble(strPoint) ;  // qry in double[]

		String winnerCell = "";
		for(int i =0 ; i <dim ;i++)
		{
			if (i ==0)
			{
				 
				winnerCell = getAddress(uniform[i], totalWidth, cellWidth);
			}
			else
			{
				 
				winnerCell +=","+getAddress(uniform[i], totalWidth, cellWidth);
			}
		}
		return winnerCell;
	}

	public static String getTransformedAddress(double[] qry, double cellWidth)
	{
		double[] standardNormal = mr.standaraizeAndremoveCorrelation(qry); // removes dependencies and normalises data
		// uniform distribution
		double[] uniform = new double[dim];
		String winnerCell = "";
		for(int i =0 ; i <dim ;i++)
		{
			if (i ==0)
			{
				uniform[i] = mr.pnorm(standardNormal[i]);
				winnerCell = getAddress(uniform[i], totalWidth, cellWidth);
			}
			else
			{
				uniform[i] = mr.pnorm(standardNormal[i]);
				winnerCell +=","+getAddress(uniform[i], totalWidth, cellWidth);
			}
		}
		return winnerCell;
	}
	
	public static double[] getTranformedCoordinates(double[] orginalPoint, double cellWidth)
	{
		double[] standardNormal = mr.standaraizeAndremoveCorrelation(orginalPoint); // removes dependencies and normalises data
		// uniform distribution
		double[] uniform = new double[dim];
		 
		for(int i =0 ; i <dim ;i++)
		{
			if (i ==0)
			{
				uniform[i] = mr.pnorm(standardNormal[i]);
				 
			}
			else
			{
				uniform[i] = mr.pnorm(standardNormal[i]);
				 
			}
		}
		return uniform;
	}

	public static double[] convertoDouble(String strLine)
	{
		String[] strPoint = strLine.split(",");
		double[] doublePoint = new double[dim];
		for (int i = 0; i < dim; i++ )
		{
			doublePoint[i] = Double.valueOf(strPoint[i]);
		}
		return doublePoint;
	}
	
	public static ArrayList<String> getFirstDimesions(double[] x, double[] y, double cellWidth)
	{
		ArrayList<String> coordinates = new ArrayList<String>();
	 	for(double i = x[0]; i < x[1] ; i+= cellWidth)
		{
			double xPrime = i;
			String strXPrime = String.valueOf(xPrime);
			for(double j = y[0]; j< y[1]  ; j+= cellWidth)
			{
				coordinates.add( strXPrime+","+String.valueOf(j));
				 
			}
			coordinates.add( strXPrime+","+String.valueOf(y[1]));
		}
		String strXPrime = String.valueOf(x[1]);
		for(double j = y[0]; j< y[1]  ; j+= cellWidth)
		{
			coordinates.add( strXPrime+","+String.valueOf(j));
		}
		coordinates.add( strXPrime+","+String.valueOf(y[1]));
		return coordinates;
	}
	
	public static ArrayList<String> getFirstDimesions(ArrayList<String> x, double[] y,double cellWidth)
	{
		ArrayList<String> coordinates = new ArrayList<String>();
	   
		for( String str : x)
		{
			for(double j = y[0]; j< y[1] ; j+= cellWidth)
			{
				coordinates.add( str+","+String.valueOf(j));
				 
			}
			coordinates.add( str+","+String.valueOf(y[1]));
		}
		return coordinates;
		
	}

}
