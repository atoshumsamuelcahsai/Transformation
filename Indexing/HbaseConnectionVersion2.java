package PIT.Indexing;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import Cache.CachInterface;
import PIT.Component;
import PIT.ProbabilityIntegralTransformation;
import PythiaHbase.AscendingHeapSort;
import PythiaHbase.DataDistance;
import PythiaHbase.DescendingHeapSort;
import PythiaHbase.Helper;

public class HbaseConnectionVersion2 implements java.io.Serializable{

	//HashMap<String ,ArrayList<String>> cont = new HashMap<String ,ArrayList<String>>();

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	HashMap<Integer, Component> components;

	public static final String HBASE_CONFIGURATION_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";

	public static final String HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT = "hbase.zookeeper.property.clientPort";

	public static final String hbaseZookeeperQuorum = "130.209.249.106,130.209.249.107,130.209.249.108,130.209.249.109,130.209.249.110";

	public static final int hbaseZookeeperClientPort=2181; Configuration conf;	Connection conn; Table table;

	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	//ProbabilityIntegralTransformation mr;   	

	int domainWidth;

	double cellWidth ;  AscendingHeapSort knn; 
	String tablename;   String outputPath;

	int valueOFK;    HashSet<String> uniqueKeys; int dimension;


	DescendingHeapSort tempknn;  ProbabilityIntegralTransformation mr;  

	public HbaseConnectionVersion2(int k, String tableName ,int width, String pathInference, String outputPath) throws IOException
	{
		this.valueOFK = k;			this.cellWidth = width; 		tablename = tableName; this.outputPath = outputPath;

		conf =   HBaseConfiguration.create(); 

		conf.set(HBASE_CONFIGURATION_ZOOKEEPER_QUORUM, hbaseZookeeperQuorum);

		conf.setInt(HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT, hbaseZookeeperClientPort);

		conn  = ConnectionFactory.createConnection(conf);

		table = conn.getTable(TableName.valueOf(tablename));

		components = new HashMap<Integer, Component>();

		Scanner sc = new Scanner(new File(pathInference));

		int count = 1;

		while (sc.hasNextLine())
		{
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
	public HbaseConnectionVersion2(int k, String tableName ,double cellwidth, int domainWidth, String covPath, String meanPath, int dimension, String outputPath) throws IOException
	{
		this.valueOFK = k; this.cellWidth = cellwidth;  this.domainWidth = domainWidth;		tablename = tableName;

		 this.outputPath = outputPath;
		
		Scanner sc = new Scanner(new File(meanPath));

		int count = 1;
		this.dimension = dimension;

		double[][] covariance = new double[dimension][dimension];
		double[] mean = new double[dimension];

		Scanner coSC = new Scanner(new File(covPath));
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

	System.out.println("reading covariance ended.....");

		Scanner meanSC = new Scanner(new File(meanPath));
		
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
		
		conf =   HBaseConfiguration.create(); 

		conf.set(HBASE_CONFIGURATION_ZOOKEEPER_QUORUM, hbaseZookeeperQuorum);

		conf.setInt(HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT, hbaseZookeeperClientPort);

		conn  = ConnectionFactory.createConnection(conf);

		table = conn.getTable(TableName.valueOf(tablename));

		components = new HashMap<Integer, Component>();

		mr = new ProbabilityIntegralTransformation(covariance, mean);
	}

	public double[] knnQuery(String query) throws IOException
	{
		uniqueKeys = new HashSet<String>(); // the first n cell that contains K number of elements are inserted her. So no need to retrieve them twice
		knn = new AscendingHeapSort(); // heap sort
		AscendingHeapSort candidates = new AscendingHeapSort();
		tempknn = new DescendingHeapSort(valueOFK);
		
		String winnerCell =  getCellKey(query, cellWidth);	// using original coordinate of the query to calculate winner cell
		
		double[] standardNormalQry = mr.standaraizeAndremoveCorrelation(convertoDouble(query));
		
		int howManyDataPointsInAWinnerCell = getDataFromHbase(winnerCell,standardNormalQry);
		 
		double radius = -1;
		 
		//calculate radius
		if(!tempknn.isEmpty() ) radius = tempknn.getDistanceToKthElement(); // k_th distance
		 
		
		double[] qry = convertoDouble(query);
		ArrayList<String> coordiantes;
		double[] tranformedUniformMinimum = new double[dimension];
		double[] tranformedUniformMaximum = new double[dimension];
		//get minimum and maximum of every dimension dimension 
		for(int i = 0 ; i < dimension; i++)
		{
			tranformedUniformMinimum[i] = standardNormalQry[i]- radius;
			tranformedUniformMaximum[i]=  standardNormalQry[i] + radius;
		}
		//transform max_min to uniform 
		tranformedUniformMinimum = getTranformedCoordinates(tranformedUniformMinimum);
		tranformedUniformMaximum = getTranformedCoordinates(tranformedUniformMaximum);
		if (qry.length <= 2)
		{
			coordiantes = concatenateTheFirstTwoDimesnions(new double[]{tranformedUniformMinimum[0], tranformedUniformMaximum[0]}, new double[]{tranformedUniformMinimum[1], tranformedUniformMaximum[1]}, cellWidth/domainWidth);
		}
		else
		{
			coordiantes = concatenateTheFirstTwoDimesnions(new double[]{tranformedUniformMinimum[0], tranformedUniformMaximum[0]}, new double[]{tranformedUniformMinimum[1], tranformedUniformMaximum[1]},cellWidth/domainWidth);
			for (int i = 2; i < qry.length; i++)
			{
				coordiantes =	concatenateTheRestDimesions(coordiantes, new double[]{tranformedUniformMinimum[i], tranformedUniformMaximum[i]}, cellWidth/domainWidth );
			}
		}
	
		
		
//		double[] standardKthdistance = mr.standaraizeAndremoveCorrelation(convertoDouble(tempknn.getstDataMaximum()));
//		 double disStandard = Helper.ecuDis(standardNormalQry, standardKthdistance);
		
		for(String key : coordiantes)
		{
 			String uniformKey = getCellKeyForTransformedCoordinates(key,cellWidth);
			
			double[] standardkey  = new double[dimension];
			double[] standardMax  = new double[dimension];
			double[] standardWidth = new double[dimension];
			double[] uniformkeydouble = convertoDouble(uniformKey);
 			double dis = 0;
			//System.out.println("uniform "+key);
			//System.out.println("Table key "+uniformKey);
			 
			for (int i = 0; i < dimension; i++)
			{
				double z = uniformkeydouble[i] / domainWidth  ;
				 
				//System.out.println(data + " ==+++===+++");
				 if (uniformkeydouble[i] == 0)
				 {
					 standardkey[i]=mr.qnorm( 0.000000000001);
					 standardMax[i]=mr.qnorm( 0.000000000001 + (cellWidth/domainWidth) );
					 standardWidth[i] = standardMax[i] - standardkey[i];
				 }
				 
				 else
				 {
					 standardkey[i] = mr.qnorm(z);
					  
					 standardMax[i]=  mr.qnorm(Math.min(z + (cellWidth/domainWidth), 0.9999999 ));
					   
					 standardWidth[i] = standardMax[i] - standardkey[i];
					
					 
				 }
				 
				 dis+= Helper.minDisPerDimension(standardNormalQry[i], standardkey[i], standardWidth[i] );
				
			}
			dis = Math.sqrt(dis);
			
			if (uniqueKeys.add(uniformKey)) 
			{
			    candidates.addNewData(uniformKey, dis);
			}
 			
		}

	 //	System.out.println("candidate cells "+longCandidateList.size() + " winner cell " + winnerCell+" Radius: "+radius);
		double msize = candidates.size();
		int retrivedCells = 0;
		while(!candidates.isEmpty())
		{
			DataDistance data = candidates.removeMin();
			
			if(data.getDistance() > tempknn.getDistanceToKthElement())
			{
				 
				// System.out.println(data.getRowKey() +"\t"+data.getDistance() +"\t"+ tempknn.getDistanceToKthElement());
			 	break;
			}
			retrivedCells++;
			if (winnerCell.compareTo(data.getRowKey())==0)	continue;
			howManyDataPointsInAWinnerCell += getDataFromHbase(data.getRowKey(), mr.standaraizeAndremoveCorrelation(convertoDouble(query)));
			
			
			//System.out.println(key + " this is my key " + howManyDataPointsInAWinnerCell);
		}
		
		while(! tempknn.isEmpty())
		{
			DataDistance data =tempknn.removeMaximum();
			knn.addNewData(data.getRowKey() , data.getDistance());
		}
		int k1 =  valueOFK;
 		//BufferedWriter bw = new BufferedWriter(new FileWriter(outputPath,true));
		while(! knn.isEmpty() && k1 > 0  )
		{
		   DataDistance data = knn.removeMin();
		  // System.out.println(data.getRowKey() + " "+ + data.getDistance() );
		  // bw.write(data.getRowKey()+","+data.getDistance()+"\n");
		   k1--;
		}
//		bw.flush();
//		bw.close();
		
//		  System.out.println(uniqueKeys.size() + "  this is size");
//			for(String x : coordiantes)
//			{
//				 System.out.println(x + "=||||||||||=");
//			}
			//add candidate cells =================== this needs updating.....
		  
		return new double[]{retrivedCells, howManyDataPointsInAWinnerCell };
	}
	public  String getAddress(Double x, int domainWidth, double cellwidth)
	{
		double y =  x * domainWidth;
		int z = (int)Math.floor(y / cellwidth);
		return String.valueOf(z * cellwidth);
	}
	public int getDataFromHbase(String key, double[] qryStandarize) throws IOException
	{
		int n = 0; // number of data elements per cell
		
		//longCandidateList.add(key);
		Get get = new Get(Bytes.toBytes(key));
		Result result = table.get(get);
		byte [] value = result.getValue(Bytes.toBytes("C"), Bytes.toBytes("x"));
		if (value==null) return 0;
		for (String data : Bytes.toString(value).split("\t"))
		{			 
			double[] standard = mr.standaraizeAndremoveCorrelation(convertoDouble(data)); 
			
			double disZscore = Helper.ecuDis(standard, qryStandarize);
				 
			tempknn.addNewData(data, disZscore);
			n++;
		}
		return n;
	}

	/*
	 * adds candidate points that resides in cells that overlaps 
	 *  the range query along with distance of the points distance from a query
	 */
	//	public int addToKNNCandidate(String key, double[] qry) throws IOException
	//	{
	//		int count =0;
	//		Get get = new Get(Bytes.toBytes(key));
	//		Result result = table.get(get);
	//		if (result==null) return 0;
	//		byte [] value = result.getValue(Bytes.toBytes("C"), Bytes.toBytes("x"));
	//		if (value==null) return 0;
	//		for (String data : Bytes.toString(value).split("\t"))
	//		{			 
	//			double dis = Helper.ecuDis(convertoDouble(data), qry);
	//			tempknn.addNewData(data, dis);
	//			count++;
	//		}
	//		//System.out.println(count);
	//		return count;
	//	}



	public void closeTable() throws IOException
	{
		this.table.close();
		this.conn.close();
		this.conf.clear();
	}


	public   String getCellKey(String strPoint,double cellWidth)
	{
		double[] qry = convertoDouble(strPoint) ;  // qry in double[]
		return getTransformedAddress(qry, cellWidth);
	}


	public String getTransformedAddress(double[] qry, double cellWidth)
	{
		double[] standardNormal = mr.standaraizeAndremoveCorrelation(qry); // removes dependencies and normalises data
		// uniform distribution
		double[] uniform = new double[dimension];
		String winnerCell = "";
		for(int i =0 ; i <dimension ;i++)
		{
			if (i ==0)
			{
				uniform[i] = mr.pnorm(standardNormal[i]);
				winnerCell = getAddress(uniform[i], domainWidth, cellWidth);
			}
			else
			{
				uniform[i] = mr.pnorm(standardNormal[i]);
				winnerCell +=","+getAddress(uniform[i], domainWidth, cellWidth);
			}
		}
		return winnerCell;
	}
	public   double[] convertoDouble(String strLine)
	{
		String[] strPoint = strLine.split(",");
		double[] doublePoint = new double[dimension];
		for (int i = 0; i < dimension; i++ )
		{
			doublePoint[i] = Double.valueOf(strPoint[i]);
		}
		return doublePoint;
	}
	public   double[] getTranformedCoordinates(double[] standardPoint )
	{
		 
		// uniform distribution
		double[] uniform = new double[dimension];
		for(int i =0 ; i <dimension;i++)
		{
			if (i ==0)
			{
				uniform[i] = mr.pnorm(standardPoint[i]);
			}
			else
			{
				uniform[i] = mr.pnorm(standardPoint[i]);
			}
		}
		return uniform;
	}
	public  ArrayList<String> concatenateTheFirstTwoDimesnions(double[] minimumPoint, double[] maximumPoint, double cellWidth)
	{
		ArrayList<String> coordinates = new ArrayList<String>();
		for(double i = minimumPoint[0]; i < minimumPoint[1] ; i+= cellWidth)
		{
			double xPrime = i;
			String strXPrime = String.valueOf(xPrime);
			for(double j = maximumPoint[0]; j< maximumPoint[1]  ; j+= cellWidth)
			{
				coordinates.add( strXPrime+","+String.valueOf(j));
			}
			coordinates.add( strXPrime+","+String.valueOf(maximumPoint[1]));
		}
		String strXPrime = String.valueOf(minimumPoint[1]);
		for(double j = maximumPoint[0]; j< maximumPoint[1]  ; j+= cellWidth)
		{
			coordinates.add( strXPrime+","+String.valueOf(j));
		}
		coordinates.add( strXPrime+","+String.valueOf(maximumPoint[1]));
		return coordinates;
	}

	public  ArrayList<String> concatenateTheRestDimesions(ArrayList<String> x, double[] y,double cellWidth)
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
	public   String getCellKeyForTransformedCoordinates(String strPoint,double cellWidth)
	{
		double[] uniform = convertoDouble(strPoint) ;  // qry in double[]
		String winnerCell = "";
		for(int i =0 ; i <dimension ;i++)
		{
			if (i ==0)
			{
				winnerCell = getAddress(uniform[i], domainWidth, cellWidth);
			}
			else
			{
				winnerCell +=","+getAddress(uniform[i], domainWidth, cellWidth);
			}
		}
		return winnerCell;
	}

//	 public double changeStandardNormal()
//	 {
//		 double[] standaredNormal = new double[dimension];
//		 for (double data : convertoDouble(uniformKey))
//			{
//				double z = data / domainWidth  ;
//				
//				//System.out.println(data + " ==+++===+++");
//				 if (data == 0)
//				 {
//					 standardkey[count]=mr.qnorm( 0.000000000001);
//				 }
//				 else
//				 {
//					 standardkey[count] = mr.qnorm(z);
//				 }
//				count++;
//			}
//	 }



}
