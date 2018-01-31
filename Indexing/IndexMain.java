package PIT.Indexing;



import java.io.File;
import java.io.IOException;


import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import BulkLoading.MapperCreateGrids;
import XmlParser.Parameters;
import XmlParser.Parser;

public class IndexMain extends Configured implements Tool {	

	static File fXmlFile;
	static DocumentBuilderFactory dbFactory;
	static DocumentBuilder dBuilder ;
	static Document doc  ;
	static NodeList nList;

	//args[0] cell width
	//args[1] domain width
	//args[2] input file path
	//args[3] output file path
	//args[4] table name
	
	//{NUMREGIONS => 150, SPLITALGO => 'HexStringSplit'}
	public static void main(String[] args)  {

		try 
		{


			int response = ToolRunner.run(new Configuration(), new IndexMain(), args);	

			if(response == 0) 
			{				
				System.out.println("Indexing and loading finished successfully...");
			} 
			else 
			{
				System.out.println("Process failed...");
			}
		} 
		catch(Exception exception) 
		{
			exception.printStackTrace();
		}
	}

	@Override
	public int run(String[] args) throws Exception {

		int result=0;

		Configuration configuration = getConf();

		//args[0] is the cell width

		
		configuration.set("cellWidth", args[0]);

		configuration.set("totalWidth", args[1]);
		
		String input = args[2];


		String output = args[3];
		
		String tableName = args[4];
		
		configuration.set("pCovarainace", args[5]);
		
		configuration.set("pMean", args[6]);
		
		configuration.set("dimension", args[7]);
		
		System.out.println(configuration.get("dimension") + " "+
				configuration.get("pMean")+ " " + configuration.get("pCovarainace")+
				" "+configuration.get("totalWidth")+" "+configuration.get("cellWidth") + "    ===============");

		configuration.set("mapred.child.java.opts", "-Xmx8192m");
		configuration.set("mapreduce.map.memory.mb", "4096");
		configuration.set("mapreduce.reduce.memory.mb", "8192");

		configuration.set("dfs.replication", "1");

		configuration.setBoolean("mapred.output.compress", false);

		FileSystem hdfs = FileSystem.get(configuration);

		 String strOutputOFIndexingProcess = args[8];
		
		//input file 
		


		Path outputPath = new Path(output);

        Path intermediatoryPath = new Path(strOutputOFIndexingProcess);
		
        if (hdfs.exists(intermediatoryPath ))  hdfs.delete(intermediatoryPath , true);

		Job job = Job.getInstance(configuration);

		      
		
		job.setJobName("PIT Indxing");

        //job.setOutputFormatClass(StreamingTextOutputFormat.class);
		FileInputFormat.addInputPaths(job, input); // use this line for single index
        
		FileOutputFormat.setOutputPath(job, intermediatoryPath );

		job.setJarByClass(getClass());

		job.setMapperClass(IndexMapper.class);

		job.setReducerClass(IndexReducer.class);
		

		job.setInputFormatClass(TextInputFormat.class);

		job.setMapOutputKeyClass(Text.class);

		job.setMapOutputValueClass(Text.class);

//		job.setOutputKeyClass(StreamingTextOutputFormat.class);
//
//		job.setOutputValueClass(StreamingTextOutputFormat.class);


		job.waitForCompletion(true);
         
		if (!job.isSuccessful()) return -1;
		
       
        
       // Path outputPath = new Path(outputOFIndexingProcess);
                
       // Configuration configuration = getConf();
        
       // FileSystem hdfs = FileSystem.get(configuration);
       
        //configuration.set("interval", args[0]);
        
        if (hdfs.exists(outputPath))  hdfs.delete(outputPath, true);
        
        Connection connection = ConnectionFactory.createConnection(configuration);
        
        HBaseConfiguration.addHbaseResources(configuration);
        
        configuration.setInt("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily", 64);
        
        configuration.set("mapred.child.java.opts", "-Xmx10g");
       
        configuration.set("dfs.replication", "1");
    
                	
        Job job2 = Job.getInstance(configuration);
             
        job2.setJobName("Bulk Loading Data");
                
        job2.setJarByClass(getClass());
        
        job2.setMapperClass(BulkLoadingMapper.class);
        
        job2.setMapOutputKeyClass(ImmutableBytesWritable.class);
	    
        job2.setMapOutputValueClass(KeyValue.class);
	    
        job2.setOutputFormatClass( HFileOutputFormat2.class);
	   
        job2.setNumReduceTasks(30);
        
     // to avoid bottleneck compress HFILE and intermediate MR data
        job2.getConfiguration().setBoolean("mapred.compress.map.output", true);
       
        job2.getConfiguration().setClass("mapred.map.output.compression.codec",GzipCodec.class,CompressionCodec.class);
        
        job2.getConfiguration().set("hfile.compression", Compression.Algorithm.LZO.getName());
        
        FileInputFormat.addInputPath(job2, new Path(strOutputOFIndexingProcess+ "/" + "part" + "*"));
        
        FileSystem.getLocal(getConf()).delete(outputPath, true);		
        
        FileOutputFormat.setOutputPath(job2, outputPath);
                
        Table table = connection.getTable(TableName.valueOf(tableName));
     	// write data directly into HBase.
        RegionLocator regionLocator = connection.getRegionLocator(table.getName());
        
        HFileOutputFormat2.configureIncrementalLoad(job2,(HTable) table,regionLocator);
        
        
     
       job2.waitForCompletion(true);
        
      if (job2.isSuccessful())
      {
    	 configuration.set("hbase.coprocessor.region.classes", "org.apache.hadoop.hbase.security.token.TokenProvider,org.apache.hadoop.hbase.security.access.AccessController");
        LoadIncrementalHFiles loader = new LoadIncrementalHFiles(configuration);
                      
        loader.doBulkLoad(outputPath,  connection.getAdmin(),(HTable) table,  regionLocator);
        
      //   FileSystem.getLocal(getConf()).delete(outputPath, true);
      }
      else
      {
    	  result = -1;
      }
        
        return result;
   



	
	}


}

