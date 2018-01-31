package PIT.Indexing;

import java.io.File;
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
import PythiaHbase.Helper;

public class HbaseConnection {
	
    //HashMap<String ,ArrayList<String>> cont = new HashMap<String ,ArrayList<String>>();
	
	HashMap<Integer, Component> components;
	
	public static final String HBASE_CONFIGURATION_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";

	public static final String HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT = "hbase.zookeeper.property.clientPort";

	public static final String hbaseZookeeperQuorum = "130.209.249.106,130.209.249.107,130.209.249.108,130.209.249.109,130.209.249.110";

	public static final int hbaseZookeeperClientPort=2181; Configuration conf;	Connection conn; Table table;
	
	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
		
	//ProbabilityIntegralTransformation mr;   	
	
	int totalWidth = 1000000;
	
	int cellWidth ;  AscendingHeapSort knn;  String tablename;
	
	int valueOFK;    HashSet<String> hs;
	 
	AscendingHeapSort tempknn;   
	
	public HbaseConnection(int k, String tableName ,int width, String pathInference) throws IOException
	{
		this.valueOFK = k;			this.cellWidth = width; 		tablename = tableName;
		
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
	
	public double[] knnQuery(String query) throws IOException
	{
		hs = new HashSet<String>(); // the first n cell that contains K number of elements are inserted her. So no need to retrieve them twice
		
		knn = new AscendingHeapSort(); // heap sort
		
		tempknn = new AscendingHeapSort(); //used to store the first  K_th data points temporarily
		
		String[] strQry = query.split(",");    // split tokens
		
		double[] qry = new double[]{Double.valueOf(strQry[0]),Double.valueOf(strQry[1])};  // qry in double[]
		
		//System.out.println(query);
		
		double[] independent =  doTransformation(new double[] {Double.valueOf(strQry[0]),Double.valueOf(strQry[1])}); // removes dependencies and normalises data
		
        String qry_x_address =  getAddress(independent[0], totalWidth, cellWidth); // get address for  x dimension based on CDF of x
		
		String qry_y_address =  getAddress(independent[1],  totalWidth, cellWidth);  // get address for y dimension based on CDF of y
		
		String winnerKey =qry_x_address+"-"+qry_y_address;
		
		int howManyDataPointsInAWinnerCell = addFirstKDataPointsToTempKNN(winnerKey,  qry);   // add  data point from the winner cell to temporary store 
		
         int winner_X_add = Integer.valueOf(qry_x_address);
		 
		 int winner_Y_add = Integer.valueOf(qry_y_address);
		 
		 int multiplier = 1;
		 
		 while (howManyDataPointsInAWinnerCell < valueOFK)
		 {
			 int width =  cellWidth * multiplier;
			 
			 howManyDataPointsInAWinnerCell += retrieveDataPointInARange(Math.max(0, winner_X_add - width ), Math.min((totalWidth - cellWidth), winner_X_add + width), Math.max(0, winner_Y_add - width ), Math.min((totalWidth - cellWidth), winner_Y_add + width), width, true, qry );
			
			 multiplier++;
			 
		 }
	
	
		double radius = -1;
		
		int k = valueOFK; 
		
		//calculate the k_th distance from a query
		while(! tempknn.isEmpty() && k >0)
		{
			
			DataDistance data =tempknn.removeMin();
			
			//System.out.println(data.getRowKey());
			
			knn.addNewData(data.getRowKey(), data.getDistance());
			
			if (k==1) radius = data.getDistance(); // k_th distance
			 
			k--;
		}
		
		// calculate range query
		double min_x = Math.max(0.0, qry[0]-radius);
		double max_x =Math.min((totalWidth - cellWidth), qry[0]+radius);
		double min_y = Math.max(0.0, qry[1]-radius);
		double max_y = Math.min((totalWidth - cellWidth), qry[1]+radius);
				
		//System.out.println(min_x + " "+ max_x + " "+ min_y + " "+ max_y);
				
		//remove correlation and standard the  minimum and maximum points of a range query.
		double[] min_min = doTransformation(new double[] {min_x, min_y});
		double[] max_max = doTransformation(new double[] {max_x, max_y});
				
		//get the address of the  four corners of the range query 
		int min_min_X_address =  Integer.valueOf(getAddress(min_min[0], totalWidth, cellWidth)  );
		int min_min_Y_address =  Integer.valueOf( getAddress(min_min[1], totalWidth, cellWidth) );
		
		int max_max_X_address =  Integer.valueOf( getAddress(max_max[0], totalWidth, cellWidth) );
		int max_max_Y_address =  Integer.valueOf( getAddress(max_max[1], totalWidth, cellWidth) );
				
		int numCells = hs.size();  // number of accessed cells
				
		// retrieve all cells that overlaps the range query
		howManyDataPointsInAWinnerCell +=retrieveDataPointInARange(Math.max(0, min_min_X_address ), Math.min((totalWidth - cellWidth), max_max_X_address), Math.max(0, min_min_Y_address ), Math.min((totalWidth - cellWidth), max_max_Y_address), cellWidth, false, qry );
	
		// print  knn
       // System.out.println("Total number of Candidate Cells "+hs.size() + " "+howManyDataPointsInAWinnerCell );
	
        
        
        // display the final kNN results.
		k = valueOFK;
		while(! knn.isEmpty() && k >0)
		{
			
			DataDistance data =knn.removeMin();
			
		 	//System.out.println(data.getRowKey() + " "+ data.getDistance());
			 
			k--;
		}
        
       
        
        return new double[]{hs.size(), howManyDataPointsInAWinnerCell };
	
	}
	
	public String getAddress(Double x, int width, int numberOFCellPerWidth)
	{
		 
		double y =  x * width;
		
		int z = (int)y / numberOFCellPerWidth;
		
		z =z * numberOFCellPerWidth;
		 
		return String.valueOf(z);
	}
	
	
	public double[] doTransformation(double[] con)
	{
		double x = 0.0;
		double y = 0.0;
		for (Integer key : components.keySet())
		{
				double[] removeCorrelation = components.get(key).standaraizeAndremoveCorrelation(new double[] {Double.valueOf(con[0]),Double.valueOf(con[1])});	
			
				x+=components.get(key).pnorm(Double.valueOf(removeCorrelation[0]));
		 
				y+=components.get(key).pnorm(Double.valueOf(removeCorrelation[1]));
				
		}
		return new double[]{x,y};
	}
	
	public int addFirstKDataPointsToTempKNN(String key, double[] qry) throws IOException
	{
	    int n = 0; // number of data elements per cell
		
	   // System.out.println(key);
	    
	    hs.add(key);
	  
	    Get get = new Get(Bytes.toBytes(key));
	    
	   
	    
	    Result result = table.get(get);

		byte [] value = result.getValue(Bytes.toBytes("C"), Bytes.toBytes("x"));
		
		 
		
		if (value==null) return 0;
			
		for (String data : Bytes.toString(value).split("\t"))
		{
		
			String[] strValue = data.split(",");
			
			double dis = Helper.ecuDis(new double[]{Double.valueOf(strValue[0]), Double.valueOf(strValue[1])}, qry);
			
			tempknn.addNewData(data, dis);
			
			n++;
		}
		
		return n;
	}
	
	public int retrieveDataPointInARange(int min_min_X_address, int max_max_X_address, int min_min_Y_address, int max_max_Y_address, int width, boolean temp, double[] qry ) throws IOException
	{
		int totalData = 0;
		
		for (int i = min_min_X_address; i <=  max_max_X_address; i+= width)
		{
			String x_address = String.valueOf(i);
			
			for(int j = min_min_Y_address; j<= max_max_Y_address; j+= width )
			{
				String y_address = String.valueOf(j);
				
				String candidatekey = x_address+"-"+y_address;
				
				if (hs.contains(candidatekey)) continue; 
				
				if (temp)
				{
					hs.add(candidatekey);
					totalData += addToKNNCandidate(candidatekey, qry,tempknn );
				}
				else
				{
					hs.add(candidatekey);
					totalData += addToKNNCandidate(candidatekey, qry,knn );
				}
        	}
			 
		}
		return totalData;
	}
	
	
	/*
	 * adds candidate points that resides in cells that overlaps 
	 *  the range query along with distance of the points distance from a query
	 */
	public int addToKNNCandidate(String key, double[] qry, AscendingHeapSort storage ) throws IOException
	{
		int count =0;
				
		Get get = new Get(Bytes.toBytes(key));
				  
		Result result = table.get(get);

		if (result==null) return 0;
		
		byte [] value = result.getValue(Bytes.toBytes("C"), Bytes.toBytes("x"));
		
		if (value==null) return 0;
		
		for (String data : Bytes.toString(value).split("\t"))
		{
			String[] strValue = data.split(",");
			
			double dis = Helper.ecuDis(new double[]{Double.valueOf(strValue[0]), Double.valueOf(strValue[1])}, qry);
			
			storage.addNewData(data, dis);
			
			count++;
			
		}
		return count;
		
	}
	
	
	
	public void closeTable() throws IOException
	{
		this.table.close();
		this.conn.close();
		this.conf.clear();
	}

}
